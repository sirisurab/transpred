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



def download_chunk_from_url(url: str, folder: str, byte_range: str, filename: str) -> str:
    try:
        #filename: str = url.split('/')[-1]+str(chunk_number)
        #ur.urlretrieve(url, folder+filename)
        request = ur.Request(url)
        request.headers['Range'] = byte_range
        request.headers['User-Agent'] = 'Mozilla/5.0'
        response = ur.urlopen(request)
        with open(folder+filename, "wb") as f:
            f.write(response.read())
        print('downloaded file to '+folder+filename)

    except Exception as err:

        raise err
    else:
        return filename


