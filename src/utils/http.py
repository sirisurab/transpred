import urllib.request as ur
import urllib.error as u_err


def download_from_url(url: str, folder: str) -> str:
    try:
        filename: str = url.split('/')[-1]
        ur.urlretrieve(url, folder+filename)
        print('downloaded file to '+folder+filename)

    except Exception as err:

        raise err
    else:
        return filename


def download_from_url_transit(url: str, folder: str) -> str:
    filename: str = url.split('/')[-1]

    try:
        ur.urlretrieve(url, folder+filename)
        print('downloaded file to '+folder+filename)

    except u_err.HTTPError as err:
        # ignore bad urls
        if err.code == 404:
            print('ignoring bad transit url '+url)
            pass
        else:
            raise err

    except Exception as err:
        raise err

    return filename
