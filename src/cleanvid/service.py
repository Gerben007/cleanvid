#!/usr/bin/env python3
"""Simple folder-watcher service to run cleanvid on new files.

Watches multiple directories and keeps a small JSON database of processed files
so files are not reprocessed unless they change (size/mtime) or their matching
subtitle file changes.

Usage: run as module: `python -m cleanvid.service`

Environment variables:
- CLEANVID_WATCH_DIRS (comma-separated list; default `/data/in,/data/usb1,/data/usb2`)
- CLEANVID_OUTPUT_DIR (default `/data/out`)
- CLEANVID_PROCESSED_DIR (default `/data/processed`)
- CLEANVID_POLL_INTERVAL (seconds, default `10`)
- CLEANVID_PRESERVE_INPUT (true/false, default `false`)
- CLEANVID_SWEARS_FILE (optional path to override swears.txt)
- CLEANVID_DB (path to processed DB file; default `/data/.cleanvid_processed.json`)
"""
import os
import time
import shutil
import subprocess
import logging
import json

VIDEO_EXTS = ('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.m4v')

log = logging.getLogger('cleanvid.service')


def env(name, default=None):
	return os.environ.get(name, default)


def ensure_dir(path):
	os.makedirs(path, exist_ok=True)


def find_videos(path):
	if not os.path.isdir(path):
		return
	for entry in os.listdir(path):
		if entry.startswith('.'):
			continue
		full = os.path.join(path, entry)
		if os.path.isfile(full) and entry.lower().endswith(VIDEO_EXTS):
			yield full


def find_subs(path):
	if not os.path.isdir(path):
		return
	for entry in os.listdir(path):
		if entry.startswith('.'):
			continue
		full = os.path.join(path, entry)
		if os.path.isfile(full) and entry.lower().endswith('.srt'):
			yield full


def process(video_path, srt_path, output_dir, processed_dir, swears_file, preserve_input=False, write_next_to_input=True):
	base = os.path.splitext(os.path.basename(video_path))[0]
	ext = os.path.splitext(video_path)[1]
	if write_next_to_input:
		out_file = os.path.join(os.path.dirname(video_path), base + '_clean' + ext)
	else:
		out_file = os.path.join(output_dir, base + '_clean' + ext)

	cmd = ['cleanvid', '-i', video_path, '-o', out_file, '-w', swears_file]
	if srt_path and os.path.isfile(srt_path):
		cmd.extend(['-s', srt_path])

	log.info('Running: %s', ' '.join(cmd))
	try:
		subprocess.run(cmd, check=True, capture_output=True, text=True)
		log.info('Processed %s -> %s', video_path, out_file)
		if not preserve_input:
			ensure_dir(processed_dir)
			try:
				shutil.move(video_path, os.path.join(processed_dir, os.path.basename(video_path)))
			except Exception:
				log.exception('Could not move processed video')
			if srt_path and os.path.isfile(srt_path):
				try:
					shutil.move(srt_path, os.path.join(processed_dir, os.path.basename(srt_path)))
				except Exception:
					log.exception('Could not move processed subtitle')
		return True
	except subprocess.CalledProcessError as e:
		log.error('Failed processing %s: %s', video_path, e.stderr or e)
		return False


def main():
	logging.basicConfig(level=logging.INFO)

	watch_dirs_env = env('CLEANVID_WATCH_DIRS', '/data/in,/data/usb1,/data/usb2')
	watch_dirs = [d.strip() for d in watch_dirs_env.split(',') if d.strip()]
	output_dir = env('CLEANVID_OUTPUT_DIR', '/data/out')
	processed_dir = env('CLEANVID_PROCESSED_DIR', '/data/processed')
	poll_interval = float(env('CLEANVID_POLL_INTERVAL', '10'))
	# default to preserving input files (do not move them) and writing outputs next to inputs
	preserve_input = env('CLEANVID_PRESERVE_INPUT', 'true').lower() in ('1', 'true', 'yes')
	write_next_to_input = env('CLEANVID_WRITE_OUTPUT_NEXT_TO_INPUT', 'true').lower() in ('1', 'true', 'yes')

	swears_file = env('CLEANVID_SWEARS_FILE')
	if not swears_file:
		mounted = os.path.join('/data', 'swears.txt')
		if os.path.isfile(mounted):
			swears_file = mounted
		else:
			swears_file = os.path.join(os.path.dirname(__file__), 'swears.txt')

	processed_db = env('CLEANVID_DB', os.path.join('/data', '.cleanvid_processed.json'))

	def load_db():
		try:
			if os.path.isfile(processed_db):
				with open(processed_db, 'r', encoding='utf-8') as fh:
					return json.load(fh)
		except Exception:
			log.exception('Could not load processed DB')
		return {}

	def save_db(db):
		try:
			tmp = processed_db + '.tmp'
			with open(tmp, 'w', encoding='utf-8') as fh:
				json.dump(db, fh, indent=2)
			os.replace(tmp, processed_db)
		except Exception:
			log.exception('Could not save processed DB')

	def stat_info(path):
		try:
			st = os.stat(path)
			return {'size': st.st_size, 'mtime': int(st.st_mtime)}
		except Exception:
			return None

	db = load_db()

	for d in watch_dirs:
		ensure_dir(d)
	ensure_dir(output_dir)
	ensure_dir(processed_dir)

	log.info('Starting cleanvid service: watch_dirs=%s output=%s', ','.join(watch_dirs), output_dir)

	while True:
		try:
			video_map = {}
			subs_map = {}
			for d in watch_dirs:
				for v in find_videos(d):
					video_map[os.path.splitext(os.path.basename(v))[0]] = v
				for s in find_subs(d):
					subs_map[os.path.splitext(os.path.basename(s))[0]] = s

			for base, vpath in list(video_map.items()):
				srt = subs_map.get(base)

				try:
					s1 = os.path.getsize(vpath)
					time.sleep(1)
					s2 = os.path.getsize(vpath)
				except OSError:
					continue
				if s1 != s2:
					continue

				key = os.path.abspath(vpath)
				vstat = stat_info(vpath)
				sstat = stat_info(srt) if srt else None

				entry = db.get(key)
				needs = False
				if not entry:
					needs = True
				else:
					if entry.get('video') != vstat:
						needs = True
					else:
						if entry.get('subs') != sstat:
							needs = True

				if not needs:
					continue

				ok = process(vpath, srt, output_dir, processed_dir, swears_file, preserve_input, write_next_to_input)
				if ok:
					db[key] = {'video': vstat, 'subs': sstat, 'processed_at': int(time.time())}
					save_db(db)
		except Exception:
			log.exception('Error in service loop')
		time.sleep(poll_interval)


if __name__ == '__main__':
	main()
*** End Patch
