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


