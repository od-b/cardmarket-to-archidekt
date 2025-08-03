import asyncio
import shutil
from pathlib import Path

import aiofiles.os


async def async_move_and_mkdir(src: Path, dst: Path):
    """
    Move from 'src' to 'dst'.

    Directories up to and including 'dst' is created if they do not exist.
    """

    await aiofiles.os.makedirs(dst, exist_ok=True)

    # Copy metadata (mode, times, flags, etc.)
    await asyncio.to_thread(
        shutil.copy2,
        src=src,
        dst=dst,
        follow_symlinks=False,
    )

    await aiofiles.os.remove(src)

    return src, dst
