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
    return NamedTuple, Optional, Path, Sequence, aioftp, asyncio, mo, obstore


@app.cell
def _(mo):
    mo.md(
        r"""DWD's HTTPS server doesn't support `PROPFIND` (which is the HTTP method used by `obstore.HTTPStore.list`). So we use DWD's FTP server instead."""
    )
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
def _(Optional, Path, Sequence, aioftp, asyncio):
    class Archivist:
        async def _download_file(self, ftp_file_path: str, dst_path: Path) -> None:
            """Establishes a new connection to download a single file."""
            async with aioftp.Client.context(host=self.ftp_host_url()) as client:
                # Use the filename as the destination path
                filename = Path(ftp_file_path).name
                destination_file = dst_path / filename

                # Ensure the destination directory exists
                destination_file.parent.mkdir(parents=True, exist_ok=True)

                print(f"Starting download: {ftp_file_path} to {destination_file}")
                await client.download(
                    source=ftp_file_path, destination=destination_file, write_into=True
                )
                print(f"Finished download: {ftp_file_path}")

        async def download_all_files_for_nwp_variable(
            self, nwp_model_run: int, nwp_variable: str, dst_path: Path
        ) -> None:
            """Download all files in the directory for this NWP variable."""
            ftp_file_paths = []
            async with aioftp.Client.context(host=self.ftp_host_url()) as client:
                ftp_dir_path = self.ftp_path(nwp_model_run, nwp_variable)
                ftp_file_paths = [path for path, _ in (await client.list(ftp_dir_path))]

            # TODO: Tidy up this hacky way of limiting concurrency, and enable the code to download more than 16 files,
            # whilst not exceeding concurrency!
            tasks = []
            for i, ftp_file_path in enumerate(ftp_file_paths):
                task = asyncio.create_task(self._download_file(ftp_file_path, dst_path))
                tasks.append(task)
                if i > 16:
                    break

            await asyncio.gather(*tasks)


    class DwdArchivist(Archivist):
        def ftp_host_url(self) -> str:
            return "opendata.dwd.de"

        def ftp_path(self, nwp_model_run: int, nwp_variable: Optional[str] = None) -> str:
            p = f"/weather/nwp/{self.nwp_id()}/grib/{nwp_model_run:02d}/"
            if nwp_variable is not None:
                p += f"{nwp_variable}/"
            return p

        def nwp_model_runs_to_archive(self) -> Sequence[int]:
            return (0, 6, 12, 18)


    class DwdIconEuArchivist(DwdArchivist):
        def nwp_id(self) -> str:
            return "icon-eu"
    return (DwdIconEuArchivist,)


@app.cell
async def _(DwdIconEuArchivist, Path):
    await DwdIconEuArchivist().download_all_files_for_nwp_variable(
        nwp_model_run=0,
        nwp_variable="alb_rad",
        dst_path=Path("/home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script"),
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
