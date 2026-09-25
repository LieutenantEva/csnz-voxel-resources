#!/usr/bin/env python3

import asyncio
import hashlib
import os
from pathlib import Path

import aiohttp


# ============================================================
# Configuration
# ============================================================

API_URL = (
    "http://52.28.231.59:3000/v5/slots/list/search"
)

LIMIT = 100

BUCKET = "nxe-cz-prod-studio"

RESOURCE_URL = (
    f"https://{BUCKET}.s3.amazonaws.com/"
    "resources_prod/{}.vxl"
)

IMAGE_URL = (
    f"https://{BUCKET}.s3.amazonaws.com/"
    "images_prod/{}.vmg"
)

RESOURCE_DIR = Path(
    "./resources_prod/"
)

IMAGE_DIR = Path(
    "./images_prod/"
)


# False:
#   Existing files are checked using MD5 + If-None-Match
#
# True:
#   Always download and replace files
OVERWRITE = False


# Maximum concurrent S3 downloads
CONCURRENCY = 8


# Retry count
RETRIES = 3


# Network chunk size
CHUNK_SIZE = 1024 * 1024  # 1MB


# Hardcoded list
hc_maps = [
    "01511948735459999001",
    "01511948343414999001",
    "01511948840574999001",
    "01511948566862999001",
    "01511948603996999001",
    "01511948771687999001",
    "01553060153843999001",
    "01698816168929999001",
    "01511948385323999001",
    "01511948417019999001",
    "01590548273374999001",
    "01511948475121999001",
    "01511948685755999001",
    "01528349418386999001",
    "01511948648886999001",
    "01530684801734999001",
    "01511948800939999001",
    "01530684672955999001",
    "01511948531101999001",
    "01553060167327999001",
]
hc_images = [
    "01511948735281999001",
    "01511948343992999001",
    "01511948839844999001",
    "01511948567662999001",
    "01511948604102999001",
    "01511948770479999001",
    "01553060152015999001",
    "01698816169372999001",
    "01511948385383999001",
    "01511948418584999001",
    "01590548273375999001",
    "01511948476111999001",
    "01511948685557999001",
    "01528349414181999001",
    "01511948649203999001",
    "01530684799124999001",
    "01511948801096999001",
    "01530684672174999001",
    "01511948532409999001",
    "01553060164764999001",
]


RESOURCE_DIR.mkdir(
    parents=True,
    exist_ok=True
)

IMAGE_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# Helpers
# ============================================================

def calculate_md5(
    path: Path,
):
    """
    Calculate local file MD5.

    NOTE:
    S3 ETag normally matches MD5 for single-part uploads.
    Multipart-uploaded objects may have non-MD5 ETags.
    This script assumes ETag behaves like MD5 as requested.
    """

    md5 = hashlib.md5()

    with open(
        path,
        "rb",
    ) as file:

        while chunk := file.read(
            CHUNK_SIZE
        ):
            md5.update(chunk)

    return md5.hexdigest()


# ============================================================
# Phase 1: API crawl
# ============================================================

async def fetch_page(
    session,
    offset,
):

    params = {
        "limit": LIMIT,
        "offset": offset,
    }


    async with session.get(
        API_URL,
        params=params,
    ) as response:

        response.raise_for_status()

        data = await response.json()

        if not data.get("succeed"):
            raise RuntimeError(
                f"API failed at offset={offset}"
            )

        return data.get(
            "result",
            []
        )


async def collect_ids():

    resource_ids = set(hc_maps)
    image_ids = set(hc_images)


    async with aiohttp.ClientSession() as session:

        offset = 0


        while True:

            print(
                f"[API] offset={offset}"
            )


            slots = await fetch_page(
                session,
                offset,
            )


            if not slots:
                break


            for slot in slots:

                resource_id = (
                    slot.get("resource_id")
                )

                image_id = (
                    slot.get("image_id")
                )


                if resource_id:
                    resource_ids.add(
                        resource_id
                    )


                if image_id:
                    image_ids.add(
                        image_id
                    )


            print(
                f"  resources={len(resource_ids)} "
                f"images={len(image_ids)}"
            )


            offset += 1


    return (
        resource_ids,
        image_ids,
    )


# ============================================================
# Phase 2: Async downloader
# ============================================================

async def download_file(
    session,
    semaphore,
    url,
    output_path,
):

    headers = {}


    if output_path.exists() and not OVERWRITE:

        local_md5 = calculate_md5(
            output_path
        )


        headers["If-None-Match"] = (
            f'"{local_md5}"'
        )


        print(
            f"[CHECK] {output_path.name} "
            f"md5={local_md5}"
        )


    temp_path = Path(
        str(output_path)
        +
        ".part"
    )


    async with semaphore:

        for attempt in range(
            1,
            RETRIES + 1,
        ):

            try:

                async with session.get(
                    url,
                    headers=headers,
                ) as response:


                    if response.status == 304:

                        print(
                            f"[UNCHANGED] "
                            f"{output_path.name}"
                        )

                        return True


                    if response.status == 404:

                        print(
                            f"[404] {url}"
                        )

                        return False


                    response.raise_for_status()


                    print(
                        f"[DOWNLOAD] "
                        f"{output_path.name}"
                    )


                    with open(
                        temp_path,
                        "wb",
                    ) as file:


                        async for chunk in (
                            response.content.iter_chunked(
                                CHUNK_SIZE
                            )
                        ):

                            file.write(
                                chunk
                            )


                os.replace(
                    temp_path,
                    output_path,
                )


                print(
                    f"[DONE] "
                    f"{output_path.name}"
                )


                return True


            except Exception as error:

                print(
                    f"[ERROR] "
                    f"{output_path.name}: "
                    f"{error}"
                )


                if temp_path.exists():

                    temp_path.unlink()


                if attempt < RETRIES:

                    await asyncio.sleep(
                        2
                    )


    print(
        f"[FAILED] "
        f"{output_path.name}"
    )

    return False



async def download_all(
    resource_ids,
    image_ids,
):

    jobs = []


    for resource_id in resource_ids:

        jobs.append(
            (
                RESOURCE_URL.format(
                    resource_id
                ),
                RESOURCE_DIR / (
                    f"{resource_id}.vxl"
                ),
            )
        )


    for image_id in image_ids:

        jobs.append(
            (
                IMAGE_URL.format(
                    image_id
                ),
                IMAGE_DIR / (
                    f"{image_id}.vmg"
                ),
            )
        )


    print()
    print(
        "=============================="
    )

    print(
        f"Files queued: {len(jobs)}"
    )

    print(
        f"Concurrency: {CONCURRENCY}"
    )

    print(
        f"Overwrite: {OVERWRITE}"
    )

    print(
        "=============================="
    )


    semaphore = asyncio.Semaphore(
        CONCURRENCY
    )


    timeout = aiohttp.ClientTimeout(
        total=None,
        sock_connect=30,
        sock_read=120,
    )


    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY
    )


    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:


        tasks = [
            download_file(
                session,
                semaphore,
                url,
                path,
            )
            for url, path in jobs
        ]


        results = await asyncio.gather(
            *tasks
        )


    success = sum(
        1
        for result in results
        if result
    )


    print()
    print(
        "=============================="
    )

    print(
        "Finished"
    )

    print(
        f"Success: {success}"
    )

    print(
        f"Failed : {len(results)-success}"
    )

    print(
        "=============================="
    )


# ============================================================
# Main
# ============================================================

async def main():

    print(
        "Collecting IDs..."
    )


    resource_ids, image_ids = (
        await collect_ids()
    )


    print()
    print(
        "=============================="
    )

    print(
        "API crawl complete"
    )

    print(
        f"Unique resources: {len(resource_ids)}"
    )

    print(
        f"Unique images:    {len(image_ids)}"
    )

    print(
        "=============================="
    )


    await download_all(
        resource_ids,
        image_ids,
    )


if __name__ == "__main__":

    asyncio.run(
        main()
    )
