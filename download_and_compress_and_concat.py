import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import obstore, obstore.store
    import aioftp
    from pathlib import Path
    from typing import Optional
    from collections.abc import Sequence
    import asyncio
    return Optional, Path, Sequence, aioftp, asyncio, mo


@app.cell
def _(mo):
    mo.md(
        r"""DWD's HTTPS server doesn't support `PROPFIND` (which is the HTTP method used by `obstore.HTTPStore.list`). So we use DWD's FTP server instead."""
    )
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
