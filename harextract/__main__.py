from __future__ import annotations

import logging
import pathlib
import shlex
import subprocess

import m3u8

import harextract
import harextract.extractor
import harextract.log

_logger = logging.getLogger(harextract.__name__)


def create_parser():
    import argparse

    root_parser = argparse.ArgumentParser("harextract")
    root_parser.add_argument(
        "file",
        type=pathlib.Path,
        help="The json file to extract the segments from",
    )
    root_parser.add_argument(
        "-o",
        "--output",
        type=pathlib.Path,
        default="./output.mp4",
        help="output directory to write the data to (default: %(default)s)",
    )
    root_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose mode",
    )

    return root_parser


def _main():
    args = create_parser().parse_args()

    harextract.log.setup(name_length=20, debug=args.verbose)
    _logger.info(f"{harextract.__name__} v{harextract.__version__}")

    output = pathlib.Path(args.output.as_posix())
    tmp = output.parent / f"{output.name}-tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    def extract_filter(path: pathlib.PurePosixPath):
        return path.suffix in (".m3u8", ".ts")

    segments = set()
    playlist = None
    playlist_path = None
    with pathlib.Path(args.file.as_posix()).open("rb") as lines:
        a = harextract.extractor.iterextract(lines, extract_filter)
        for url, data in a:
            if url.suffix == ".m3u8":
                playlist = m3u8.loads(data.decode())
                for segment in playlist.segments:
                    if segment.uri:
                        segment.base_uri = None
                        segment.uri = pathlib.PurePosixPath(segment.uri).name
                playlist_path = tmp / url.name
                playlist.dump(playlist_path)
            else:
                segments.add(url.name)
                with (tmp / url.name).open("wb") as file:
                    file.write(data)

    if not playlist or not playlist_path:
        _logger.warning("No playlist found, not concatenating")
        return

    playlist_segments = {segment.uri for segment in playlist.segments}
    if missing_segments := playlist_segments.difference(segments):
        if len(missing_segments) > 10:
            _logger.error("More than 10 segments missing, not concatenating")
            return

        _logger.warning("Missing segments: %s", ", ".join(sorted(missing_segments)))

    _logger.info("Successfully exported fragments")

    ffmpeg_args = ["ffmpeg", "-hide_banner", "-stats"]
    if not args.verbose:
        ffmpeg_args.extend(["-loglevel", "warning"])
    ffmpeg_args.extend(["-i", str(playlist_path)])
    ffmpeg_args.extend(["-c", "copy"])
    ffmpeg_args.extend(["-map", "0:v"])
    ffmpeg_args.extend(["-map", "0:a"])
    ffmpeg_args.extend(["-bsf:a", "aac_adtstoasc"])
    ffmpeg_args.append(str(args.output))

    _logger.debug("Running ffmpeg: %s", shlex.join(ffmpeg_args))
    subprocess.run(ffmpeg_args, check=False)


def main():
    try:
        _main()
    except KeyboardInterrupt:
        _logger.info("Interrupted by user")
    except Exception:
        _logger.exception("Unexpected exception")


if __name__ == "__main__":
    main()
