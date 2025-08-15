import os
import re
import asyncio
import logging
import shutil
from bot.helpers.utils import (
    run_apple_downloader,
    extract_apple_metadata,
    send_message,
    edit_message,
    format_string,
    cleanup,
    list_apple_output_files,
    cleanup_apple_global
)
from bot.helpers.uploader import track_upload, album_upload, music_video_upload, artist_upload, playlist_upload
from bot.helpers.database.pg_impl import download_history
from config import Config
from bot.logger import LOGGER

logger = logging.getLogger(__name__)

class AppleMusicProvider:
    def __init__(self):
        self.name = "apple"
    
    def validate_url(self, url: str) -> bool:
        """Check if URL is valid Apple Music content"""
        return bool(re.match(
            r"https://music\.apple\.com/.+/(album|song|playlist|music-video)/.+", 
            url
        ))
    
    def extract_content_id(self, url: str) -> str:
        """Extract Apple Music content ID from URL"""
        match = re.search(r'/(album|song|playlist|music-video|artist)/[^/]+/(\d+)', url)
        return match.group(2) if match else "unknown"
    
    async def process(self, url: str, user: dict, options: dict = None) -> dict:
        """Process Apple Music URL with options"""
        # Session mode handling
        from bot.settings import bot_set
        session_mode = getattr(bot_set, 'apple_session_mode', getattr(Config, 'APPLE_SESSION_MODE', 'GLOBAL')).upper()
        # Create user-specific base directory
        user_dir = os.path.join(Config.LOCAL_STORAGE, str(user['user_id']), "Apple Music")
        os.makedirs(user_dir, exist_ok=True)
        # Create per-message session directories when using session modes
        session_root = os.path.join(user_dir, str(user.get('r_id', 'unknown')))
        session_cfg_dir = os.path.join(session_root, 'session')
        session_alac = os.path.join(session_cfg_dir, 'alac')
        session_atmos = os.path.join(session_cfg_dir, 'atmos')
        session_aac = os.path.join(session_cfg_dir, 'aac')
        if session_mode != 'GLOBAL':
            for p in (session_alac, session_atmos, session_aac):
                os.makedirs(p, exist_ok=True)
            # Prepare session config.yaml (use absolute paths)
            session_cfg_path = os.path.join(session_cfg_dir, 'config.yaml')
            try:
                abs_alac = os.path.abspath(session_alac)
                abs_atmos = os.path.abspath(session_atmos)
                abs_aac = os.path.abspath(session_aac)
                with open(session_cfg_path, 'w') as f:
                    f.write(f"alac-save-folder: {abs_alac}\n")
                    f.write(f"atmos-save-folder: {abs_atmos}\n")
                    f.write(f"aac-save-folder: {abs_aac}\n")
                LOGGER.info(f"Session config written: {session_cfg_path}")
            except Exception as e:
                LOGGER.error(f"Failed to write session config: {e}")
        
        # Process options
        cmd_options = self.build_options(options)

        # Initialize progress reporter
        from bot.helpers.progress import ProgressReporter
        label = f"Apple Music • ID: {user.get('task_id','?')}"
        reporter = ProgressReporter(user['bot_msg'], label=label)
        user['progress'] = reporter
        await reporter.set_stage("Preparing")
        
        # Download content
        if session_mode == 'GLOBAL':
            result = await run_apple_downloader(
                url,
                user_dir,
                cmd_options,
                user,
                progress=reporter,
                task_id=user.get('task_id'),
                cancel_event=user.get('cancel_event')
            )
        else:
            # Two session modes supported:
            # - SESSION_CWD: run compiled binary; override HOME so downloader reads $HOME/amalac/config.yaml mapped to session config
            # - SESSION_SYMLINK: run `go run .`; override HOME similarly (no repo symlink needed)
            go_repo_dir = os.path.expanduser("~/amalac")
            go_binary = "/usr/local/go/bin/go"
            import asyncio as _asyncio
            # Prepare isolated HOME so downloader reads session config at $HOME/amalac/config.yaml
            ttemp_home = os.path.abspath(os.path.join(session_cfg_dir, '.home'))
            os.makedirs(os.path.join(ttemp_home, 'amalac'), exist_ok=True)
            session_cfg_path = os.path.join(session_cfg_dir, 'config.yaml')
            amalac_cfg_target = os.path.join(ttemp_home, 'amalac', 'config.yaml')
            # Write a tiny proxy config that points to absolute session paths (already absolute)
            if not os.path.exists(amalac_cfg_target):
                try:
                    with open(session_cfg_path, 'r') as src, open(amalac_cfg_target, 'w') as dst:
                        dst.write(src.read())
                except Exception as e:
                    return {'success': False, 'error': f'Failed to prepare session HOME config: {e}'}
            # Build env with overridden HOME
            env_vars = os.environ.copy()
            env_vars['HOME'] = ttemp_home
            if session_mode == 'SESSION_CWD':
                # Build the binary once if missing
                built_bin = os.path.join(go_repo_dir, "amdl")
                try:
                    if not os.path.exists(built_bin):
                        LOGGER.info("Building Apple downloader binary for SESSION_CWD mode...")
                        proc_build = await _asyncio.create_subprocess_exec(
                            go_binary, "build", "-o", built_bin, ".",
                            stdout=_asyncio.subprocess.PIPE,
                            stderr=_asyncio.subprocess.PIPE,
                            cwd=go_repo_dir,
                            env=env_vars
                        )
                        _, build_err = await proc_build.communicate()
                        if proc_build.returncode != 0:
                            result = {'success': False, 'error': build_err.decode().strip() or 'go build failed'}
                        else:
                            result = {'success': True}
                    else:
                        result = {'success': True}
                except Exception as e:
                    result = {'success': False, 'error': str(e)}
                # Run the binary
                if result.get('success'):
                    cmd = [built_bin]
                    if cmd_options:
                        cmd.extend(cmd_options)
                    cmd.append(url)
                    LOGGER.info(f"Running Apple downloader (session mode SESSION_CWD) with binary (HOME isolated)")
                    try:
                        proc = await _asyncio.create_subprocess_exec(
                            *cmd,
                            stdout=_asyncio.subprocess.PIPE,
                            stderr=_asyncio.subprocess.PIPE,
                            env=env_vars
                        )
                        stdout, stderr = await proc.communicate()
                        if proc.returncode != 0:
                            LOGGER.error(f"Apple downloader failed (session): {stderr.decode().strip() or stdout.decode().strip()}")
                            result = {'success': False, 'error': stderr.decode().strip() or stdout.decode().strip()}
                        else:
                            result = {'success': True}
                    except Exception as e:
                        result = {'success': False, 'error': str(e)}
            else:
                # SESSION_SYMLINK (or any other non-GLOBAL session) uses go run with isolated HOME
                cmd = [go_binary, "run", "."]
                if cmd_options:
                    cmd.extend(cmd_options)
                cmd.append(url)
                LOGGER.info(f"Running Apple downloader (session mode {session_mode}) with go run (HOME isolated)")
                try:
                    proc = await _asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=_asyncio.subprocess.PIPE,
                        stderr=_asyncio.subprocess.PIPE,
                        cwd=go_repo_dir,
                        env=env_vars
                    )
                    stdout, stderr = await proc.communicate()
                    if proc.returncode != 0:
                        LOGGER.error(f"Apple downloader failed (session): {stderr.decode().strip() or stdout.decode().strip()}")
                        result = {'success': False, 'error': stderr.decode().strip() or stdout.decode().strip()}
                    else:
                        result = {'success': True}
                except Exception as e:
                    result = {'success': False, 'error': str(e)}
        if not result['success']:
            LOGGER.error(f"Apple downloader failed: {result['error']}")
            # On error in session mode, cleanup session folder
            if session_mode != 'GLOBAL':
                try:
                    shutil.rmtree(session_root, ignore_errors=True)
                except Exception:
                    pass
            return result
        
        # Find downloaded files
        if session_mode == 'GLOBAL':
            files = list_apple_output_files()
        else:
            # List only in session folders
            files = []
            for base in (session_alac, session_atmos, session_aac):
                for root, _, names in os.walk(base):
                    for name in names:
                        if name.lower().endswith(('.m4a', '.flac', '.alac', '.mp4', '.m4v', '.mov')):
                            files.append(os.path.join(root, name))
        
        if not files:
            if session_mode == 'GLOBAL':
                LOGGER.error("No files found in global Apple output folders")
            else:
                LOGGER.error("No files found in session Apple output folders")
            return {'success': False, 'error': "No files downloaded"}
        
        if session_mode == 'GLOBAL':
            LOGGER.info(f"Found {len(files)} files in global Apple output folders")
        else:
            LOGGER.info(f"Found {len(files)} files in session Apple output folders")
        
        # Extract metadata
        items = []
        for file_path in files:
            try:
                metadata = await extract_apple_metadata(file_path)
                metadata['filepath'] = file_path
                metadata['provider'] = self.name
                items.append(metadata)
                LOGGER.info(f"Processed file: {file_path}")
            except Exception as e:
                LOGGER.error(f"Metadata extraction failed for {file_path}: {str(e)}")
        
        # Handle case where no metadata was extracted
        if not items:
            LOGGER.error("No valid metadata extracted for any files")
            return {'success': False, 'error': "Metadata extraction failed"}
        
        # Update progress with total tracks
        try:
            await user['progress'].set_total_tracks(len(items))
            await user['progress'].update_download(tracks_done=len(items))
        except Exception:
            pass
        
        # Determine content type based on file types
        has_video = any(f.endswith(('.mp4', '.m4v', '.mov')) for f in files)
        has_audio = any(f.endswith(('.m4a', '.flac', '.alac')) for f in files)
        is_single = len(items) == 1
        
        if is_single:
            if has_video:
                content_type = 'video'
                folder_path = os.path.dirname(items[0]['filepath'])
            else:
                content_type = 'track'
                folder_path = os.path.dirname(items[0]['filepath'])
        elif has_video and has_audio:
            # Mixed content - treat as playlist
            content_type = 'playlist'
            folder_path = os.path.dirname(os.path.commonpath([i['filepath'] for i in items]))
            LOGGER.warning(f"Mixed video/audio content detected. Treating as playlist: {folder_path}")
        else:
            # Pure audio collection
            content_type = 'album'
            folder_path = os.path.dirname(os.path.commonpath([i['filepath'] for i in items]))
        
        # Record download in history
        content_id = self.extract_content_id(url)
        quality = options.get('mv-max', Config.APPLE_ATMOS_QUALITY) if has_video else \
                 options.get('alac-max', Config.APPLE_ALAC_QUALITY) if 'alac' in (options or {}) else \
                 options.get('atmos-max', Config.APPLE_ATMOS_QUALITY)
        
        # Use first item's title if album title is missing
        album_title = items[0].get('album', items[0]['title'])
        
        download_history.record_download(
            user_id=user['user_id'],
            provider=self.name,
            content_type=content_type,
            content_id=content_id,
            title=album_title,
            artist=items[0]['artist'],
            quality=str(quality)  # Convert to string
        )
        
        return {
            'success': True,
            'type': content_type,
            'items': items,
            'folderpath': folder_path,
            'title': album_title,
            'artist': items[0]['artist'],
            'poster_msg': user['bot_msg'],
            'session_root': session_root if session_mode != 'GLOBAL' else None
        }
    
    def build_options(self, options: dict) -> list:
        """Convert options dictionary to command-line flags"""
        if not options:
            return []
        
        cmd_options = []
        option_map = {
            'aac': '--aac',
            'aac-type': '--aac-type',
            'alac-max': '--alac-max',
            'all-album': '--all-album',
            'atmos': '--atmos',
            'atmos-max': '--atmos-max',
            'debug': '--debug',
            'mv-audio-type': '--mv-audio-type',
            'mv-max': '--mv-max',
            'select': '--select',
            'song': '--song'
        }
        
        for key, value in options.items():
            if key in option_map:
                if value is True:  # Flag option
                    cmd_options.append(option_map[key])
                else:  # Value option
                    cmd_options.extend([option_map[key], str(value)])
        
        return cmd_options

async def start_apple(link: str, user: dict, options: dict = None):
    """Handle Apple Music download request with options"""
    try:
        provider = AppleMusicProvider()
        if not provider.validate_url(link):
            await edit_message(user['bot_msg'], "❌ Invalid Apple Music URL")
            return
        
        # Process content with options
        result = await provider.process(link, user, options)
        if not result['success']:
            await edit_message(user['bot_msg'], f"❌ Error: {result['error']}")
            return
        
        # Process and upload content based on type
        if result['type'] == 'track':
            await track_upload(result['items'][0], user)
        elif result['type'] == 'video':
            # Update label to show video emoji
            try:
                if user.get('progress'):
                    user['progress'].label = f"🎬 Apple Music • ID: {user.get('task_id','?')}"
            except Exception:
                pass
            await music_video_upload(result['items'][0], user)
        elif result['type'] == 'album':
            await album_upload(result, user)
        elif result['type'] == 'playlist':
            await playlist_upload(result, user)
        else:
            await edit_message(user['bot_msg'], f"❌ Unsupported content type: {result['type']}")
            return
        
        # Final cleanup
        try:
            await user['progress'].set_stage("Finalizing")
        except Exception:
            pass
        from bot.settings import bot_set
        session_mode = getattr(bot_set, 'apple_session_mode', getattr(Config, 'APPLE_SESSION_MODE', 'GLOBAL')).upper()
        if session_mode == 'GLOBAL':
            await cleanup(user)
            cleanup_apple_global()
        else:
            # Remove only session root
            try:
                # Prefer returned session_root if present
                session_root = result.get('session_root')
                if not session_root:
                    user_dir = os.path.join(Config.LOCAL_STORAGE, str(user['user_id']), "Apple Music")
                    session_root = os.path.join(user_dir, str(user.get('r_id', 'unknown')))
                shutil.rmtree(session_root, ignore_errors=True)
            except Exception:
                pass
        try:
            await user['progress'].set_stage("Done")
        except Exception:
            await edit_message(user['bot_msg'], "✅ Apple Music download completed!")
        
    except asyncio.CancelledError:
        try:
            await edit_message(user['bot_msg'], "⏹️ Task cancelled. Cleaning up…")
        except Exception:
            pass
        await cleanup(user)
        raise
    except Exception as e:
        logger.error(f"Apple Music error: {str(e)}", exc_info=True)
        try:
            await user.get('progress', None).set_stage("Done")
        except Exception:
            await edit_message(user['bot_msg'], f"❌ Error: {str(e)}")
        await cleanup(user)
