import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import ftputil
    return (ftputil,)


@app.cell
def _(ftputil):
    with ftputil.FTPHost("opendata.dwd.de", user="anonymous", passwd="guest") as _ftp_host:
        listing = _ftp_host.listdir("/weather/nwp/icon-eu/grib/09/")
    return (listing,)


@app.cell
def _(listing):
    listing
    return


@app.cell
def _(ftputil):
    with ftputil.FTPHost("opendata.dwd.de", user="anonymous", passwd="guest") as _ftp_host:
        lstat = _ftp_host.lstat("/weather/nwp/icon-eu/grib/09/alb_rad")
    return (lstat,)


@app.cell
def _(lstat):
    lstat
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
