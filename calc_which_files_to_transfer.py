import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(r"""
    # Note that this code has moved to [`reformatters` PR331](https://github.com/dynamical-org/reformatters/pull/331)!
    """)
    return


@app.cell
def _():
    import marimo as mo
    import aioftp
    from aioftp.client import UnixListInfo
    import obstore.store
    from obstore.store import ObjectStore
    from dataclasses import dataclass
    import pathlib
    from pathlib import PurePosixPath
    import re
    from datetime import datetime
    from typing import Final, NamedTuple, Sequence
    from abc import ABC, abstractmethod
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from obstore import ObjectMeta
    return (
        ABC,
        Final,
        NamedTuple,
        ObjectStore,
        PurePosixPath,
        Sequence,
        UnixListInfo,
        abstractmethod,
        aioftp,
        dataclass,
        datetime,
        mo,
        obstore,
        re,
    )


@app.cell
def _(PurePosixPath, UnixListInfo):
    type PathAndInfo = tuple[PurePosixPath, UnixListInfo]
    return (PathAndInfo,)


@app.cell
async def _(
    ABC,
    Final,
    NamedTuple,
    ObjectStore,
    PathAndInfo,
    PurePosixPath,
    Sequence,
    UnixListInfo,
    abstractmethod,
    aioftp,
    dataclass,
    datetime,
    obstore,
    re,
):
    @dataclass
    class TransferJob:
        src_ftp_path: PurePosixPath
        src_ftp_file_size_bytes: int

        # The destination path in the object store.
        # Starts at the datetime part, e.g., '2025-12-12T00Z/alb_rad/...'.
        dst_obstore_path: PurePosixPath

        nwp_init_datetime: datetime  # The NWP initialization datetime, extracted from `src_ftp_path`.


    # We use a NamedTuple so we can store `PathAndSize` objects in a set.
    class PathAndSize(NamedTuple):
        path: str
        file_size_bytes: int


    class FtpTransferCalculator(ABC):
        async def transfer_new_files_for_all_nwp_inits(self):
            ftp_paths_for_nwp_inits: list[PurePosixPath] = await self._list_ftp_paths_for_all_required_nwp_inits()

            for ftp_path in ftp_paths_for_nwp_inits:
                ftp_listing_for_nwp_init = await self._list_ftp_files_for_single_nwp_init(ftp_path)
                await self.transfer_new_files_for_single_nwp_init(ftp_listing_for_nwp_init)

        async def transfer_new_files_for_single_nwp_init(self, ftp_listing_for_nwp_init: Sequence[PathAndInfo]) -> None:
            # Collect list of TransferJobs from FTP server's listing:
            ftp_transfer_jobs: list[TransferJob] = []
            for ftp_path, ftp_info in ftp_listing_for_nwp_init:
                if not self._skip_ftp_item(ftp_path, ftp_info):
                    self._sanity_check_ftp_path(ftp_path)
                    transfer_job = self._convert_ftp_path_to_transfer_job(ftp_path, ftp_info)
                    ftp_transfer_jobs.append(transfer_job)

            # Find the earliest NWP init datetime.
            # We'll use this as the `offset` when listing objects on object storage.
            min_nwp_init_datetime = min([transfer_job.nwp_init_datetime for transfer_job in ftp_transfer_jobs])

            obstore_listing_set = self._list_obstore_files_for_single_nwp_init(min_nwp_init_datetime)

            jobs_still_to_download: list[TransferJob] = []

            for transfer_job in ftp_transfer_jobs:
                obstore_path = str(self._obstore_root_path / transfer_job.dst_obstore_path)
                obstore_path_and_size = PathAndSize(obstore_path, transfer_job.src_ftp_file_size_bytes)
                if obstore_path_and_size not in obstore_listing_set:
                    jobs_still_to_download.append(transfer_job)

            # TODO(Jack): Send `jobs_still_to_download` to FTP download client
            return jobs_still_to_download

        def _list_obstore_files_for_single_nwp_init(self, nwp_init: datetime) -> set[PathAndSize]:
            nwp_init_datetime_str = nwp_init.strftime(self._format_string_for_nwp_init_datetime_in_obstore_path)

            obstore_listing = self._object_store.list(
                prefix=str(self._obstore_root_path),
                offset=str(self._obstore_root_path / nwp_init_datetime_str),
            ).collect()

            return {PathAndSize(item["path"], item["size"]) for item in obstore_listing}

        def _convert_ftp_path_to_transfer_job(self, ftp_path: PurePosixPath, ftp_info: UnixListInfo) -> TransferJob:
            """Converts the FTP path to a `TransferJob`.

            This function is designed to work with the style of DWD FTP ICON-EU path in use in 2025, such as:
            /weather/nwp/icon-eu/grib/00/alb_rad/icon-eu_europe_regular-lat-lon_single-level_2025112600_004_ALB_RAD.grib2.bz2
            """
            nwp_init_datetime: datetime = self._extract_init_datetime_from_ftp_path(ftp_path)
            nwp_init_datetime_obstore_str = nwp_init_datetime.strftime(
                self._format_string_for_nwp_init_datetime_in_obstore_path
            )

            # Create dst_obstore_path:
            nwp_variable_name = self._extract_nwp_variable_name_from_ftp_path(ftp_path)
            dst_obstore_path = PurePosixPath(nwp_init_datetime_obstore_str) / nwp_variable_name / ftp_path.name

            file_size_bytes = int(ftp_info["size"])

            return TransferJob(
                src_ftp_path=ftp_path,
                src_ftp_file_size_bytes=file_size_bytes,
                dst_obstore_path=dst_obstore_path,
                nwp_init_datetime=nwp_init_datetime,
            )

        ################# Methods that can (optionally) be overridden ######################

        @staticmethod
        def _skip_ftp_item(ftp_path: PurePosixPath, ftp_info: UnixListInfo) -> bool:
            """Skip FTP items that we don't need."""
            if ftp_info["type"] == "dir":  # Skip directories.
                return True

            if not ftp_path.name.endswith("grib2.bz2"):
                return True

            if "pressure-level" in ftp_path.name:  # Skip pressure-level files.
                return True

            return False

        @property
        def _format_string_for_nwp_init_datetime_in_obstore_path(self) -> str:
            return "%Y-%m-%dT%HZ"

        ################ Methods that must be overridden ###################################

        @property
        @abstractmethod
        def _obstore_root_path(self) -> PurePosixPath:
            pass

        @property
        @abstractmethod
        def _object_store(self) -> ObjectStore:
            pass

        @staticmethod
        @abstractmethod
        async def _list_ftp_files_for_single_nwp_init(ftp_path: PurePosixPath) -> Sequence[PathAndInfo]:
            """List all files available on the FTP server for a single NWP init identified by the `path`."""
            pass

        @staticmethod
        @abstractmethod
        async def _list_ftp_paths_for_all_required_nwp_inits() -> Sequence[PurePosixPath]:
            pass

        @staticmethod
        @abstractmethod
        def _sanity_check_ftp_path(ftp_path: PurePosixPath) -> None:
            pass

        @staticmethod
        @abstractmethod
        def _extract_init_datetime_from_ftp_path(ftp_path: PurePosixPath) -> datetime:
            pass

        @staticmethod
        @abstractmethod
        def _extract_nwp_variable_name_from_ftp_path(ftp_path: PurePosixPath) -> str:
            pass


    class DwdFtpTransferCalculator(FtpTransferCalculator):
        @property
        def _obstore_root_path(self) -> PurePosixPath:
            # TODO(Jack): Change this after testing!
            return PurePosixPath(
                "home/jack/data/ICON-EU/grib/download_and_compress_and_concat_script/icon-eu/regular-lat-lon/"
            )

        @property
        def _object_store(self) -> ObjectStore:
            return obstore.store.LocalStore()  # TODO(Jack): Change this! LocalStore is only for testing!

        @staticmethod
        async def _list_ftp_paths_for_all_required_nwp_inits() -> Sequence[PurePosixPath]:
            """Return a sequence of paths like '/weather/nwp/icon-eu/grib/00'."""
            ROOT_PATH: Final[PurePosixPath] = PurePosixPath("/weather/nwp/icon-eu/grib")
            return [ROOT_PATH / f"{hour:02d}" for hour in (0, 6, 12, 18)]

        @staticmethod
        async def _list_ftp_files_for_single_nwp_init(ftp_path: PurePosixPath) -> Sequence[PathAndInfo]:
            """List all files available on the FTP server for a single NWP init identified by the `path`.

            List all files and directories in `path`. If `path` is a file, then result will be empty.

            This takes about 30 seconds for a single NWP init.
            """
            async with aioftp.Client.context("opendata.dwd.de") as ftp_client:
                ftp_listing = await ftp_client.list(ftp_path, recursive=True)
            return ftp_listing

        @staticmethod
        def _sanity_check_ftp_path(ftp_path: PurePosixPath) -> None:
            EXPECTED_N_PARTS: Final[int] = 8
            if len(ftp_path.parts) != EXPECTED_N_PARTS:
                raise ValueError(f"Expected the FTP path to have {EXPECTED_N_PARTS}, not {len(ftp_path.parts)}")
            if ftp_path.parts[1:3] != ("weather", "nwp"):
                raise ValueError(f"Expected the start of the FTP path to be /weather/nwp/..., not {ftp_path}")

        @staticmethod
        def _extract_init_datetime_from_ftp_path(ftp_path: PurePosixPath) -> datetime:
            # Extract the NWP init datetime string from the filename. For example, from this filename:
            #     "...lat-lon_single-level_2025112600_004_ALB_RAD.grib2.bz2"
            # Extract this:                ^^^^^^^^^^
            nwp_init_date_match = re.search(r"_(20\d{8})_", ftp_path.stem)
            if nwp_init_date_match:
                nwp_init_date_str = nwp_init_date_match.group(1)
            else:
                raise ValueError(f"Failed to match datetime string in {ftp_path.name=}")
            return datetime.strptime(nwp_init_date_str, "%Y%m%d%H")

        @staticmethod
        def _extract_nwp_variable_name_from_ftp_path(ftp_path: PurePosixPath) -> str:
            return ftp_path.parts[6]


    # Test!
    ftp_calc = DwdFtpTransferCalculator()
    ftp_paths_for_nwp_inits: list[PurePosixPath] = await ftp_calc._list_ftp_paths_for_all_required_nwp_inits()
    ftp_paths_for_nwp_inits
    return ftp_calc, ftp_paths_for_nwp_inits


@app.cell
async def _(ftp_calc, ftp_paths_for_nwp_inits: "list[PurePosixPath]"):
    ftp_listing_for_nwp_init = await ftp_calc._list_ftp_files_for_single_nwp_init(ftp_paths_for_nwp_inits[0])
    ftp_listing_for_nwp_init[0]
    return (ftp_listing_for_nwp_init,)


@app.cell
async def _(ftp_calc, ftp_listing_for_nwp_init):
    jobs_still_to_transfer = await ftp_calc.transfer_new_files_for_single_nwp_init(ftp_listing_for_nwp_init)
    return (jobs_still_to_transfer,)


@app.cell
def _(jobs_still_to_transfer):
    jobs_still_to_transfer[0]
    return


@app.cell
def _(jobs_still_to_transfer):
    jobs_still_to_transfer[1]
    return


@app.cell
def _(jobs_still_to_transfer):
    jobs_still_to_transfer[2]
    return


@app.cell
def _(jobs_still_to_transfer):
    len(jobs_still_to_transfer)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
