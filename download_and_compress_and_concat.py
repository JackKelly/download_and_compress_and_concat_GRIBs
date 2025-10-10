import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import obstore, obstore.store
    import aioftp
    from pathlib import Path
    from typing import Optional, NamedTuple
    from collections.abc import Sequence
    import asyncio
    return NamedTuple, Path, aioftp, asyncio, mo, obstore


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""DWD's HTTPS server doesn't support `PROPFIND` (which is the HTTP method used by `obstore.HTTPStore.list`). So we use DWD's FTP server instead."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Concurrent FTP download & `obstore` upload""")
    return


@app.cell
def _(NamedTuple, Path, aioftp, asyncio):
    class FtpTask(NamedTuple):
        src_path: Path
        dst_path: str
        n_retries: int = 0


    class ObstoreTask(NamedTuple):
        data: bytes
        dst_path: str
        n_retries: int = 0


    async def ftp_worker(
        worker_id: int,
        ftp_host: str,
        input_queue: asyncio.Queue[FtpTask],
        output_queue: asyncio.Queue[ObstoreTask],
    ) -> None:
        """A worker that keeps a single FTP connection alive and processes queue items."""
        print(f"Worker {worker_id}: Starting up...")

        # Establish the persistent client connection
        async with aioftp.Client.context(ftp_host) as ftp_client:
            print(f"Worker {worker_id}: Connection established and logged in.")

            # Start continuous processing loop
            while True:
                try:
                    ftp_task: FtpTask = input_queue.get_nowait()
                except asyncio.QueueEmpty:
                    print(f"Worker {worker_id}: Queue of FTP tasks is empty. Finishing.")
                    break

                print(f"Worker {worker_id}: Downloading {ftp_task=}")

                async with ftp_client.download_stream(ftp_task.src_path) as stream:
                    data = await stream.read()

                print(f"Worker {worker_id}: Finished {ftp_task=}")

                await output_queue.put(ObstoreTask(data, ftp_task.dst_path))

                input_queue.task_done()

                # TODO: Handle retries
    return FtpTask, ObstoreTask, ftp_worker


@app.cell
def _(ObstoreTask, asyncio):
    async def obstore_worker(worker_id: int, store, output_queue: asyncio.Queue[ObstoreTask]):
        while True:
            print(f"obstore_worker {worker_id}: Getting output_queue task")
            try:
                _obstore_task: ObstoreTask = await output_queue.get()
            except asyncio.QueueShutDown:
                print(f"obstore_worker {worker_id}: output_queue has shut down!")
                break

            await store.put_async(_obstore_task.dst_path, _obstore_task.data)
            output_queue.task_done()
            print(f"obstore_worker {worker_id}: Done output_queue task")

            # TODO: Handle retries
    return (obstore_worker,)


@app.cell
async def _(FtpTask, Path, asyncio, ftp_worker, obstore, obstore_worker):
    _input_queue = asyncio.Queue(maxsize=10)
    await _input_queue.put(
        FtpTask(
            src_path=Path(
                "/weather/nwp/icon-eu/grib/00/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025100900_000_ALB_RAD.grib2.bz2"
            ),
            dst_path="/home/jack/data/test.grib2.bz2",
        )
    )

    output_queue = asyncio.Queue(maxsize=10)

    store = obstore.store.LocalStore()

    # --- Start the Pipeline ---
    N_CONCURRENT_FTP_CONNECTIONS = 4
    N_CONCURRENT_OBSTORE_WORKERS = 4
    async with asyncio.TaskGroup() as tg:
        for worker_id in range(N_CONCURRENT_FTP_CONNECTIONS):
            tg.create_task(
                ftp_worker(
                    worker_id=worker_id,
                    ftp_host="opendata.dwd.de",
                    input_queue=_input_queue,
                    output_queue=output_queue,
                )
            )

        for worker_id in range(N_CONCURRENT_OBSTORE_WORKERS):
            tg.create_task(obstore_worker(worker_id, store, output_queue))

        print("await join")
        await _input_queue.join()
        print("joined input_queue!")
        output_queue.shutdown()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
