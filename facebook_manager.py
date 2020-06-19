import mimetypes
from urllib.parse import urlencode


async def upload_facebook_attachment(attachment_type, url: str, retries=5, *, config, session):
    """
    Upload attachment to Facebook to retrieve the attachment ID
    This is used for better performance
    :return: attachment_id of successful upload
    """
    if retries < 0:
        print('No more retries left.')
        return

    extension = url.split('.')[-1]
    try:
        content_type = mimetypes.types_map[f".{extension}"]
        if content_type:
            try:
                params = (('access_token', config['facebook_page_access_token']),)
                data = {'message': {"attachment": {"type": f"{attachment_type}", "payload": {"is_reusable": True,
                                                                                             "url": url}}}}
                async with session.post('https://graph.facebook.com/v7.0/me/message_attachments', params=params,
                                        json=data) as response:
                    if response.status != 200:
                        print(f'Response error from Facebook. Retries left {retries}: {url}')
                        attachment_id = await upload_facebook_attachment(attachment_type, url, retries - 1,
                                                                         config=config,
                                                                         session=session)
                        return attachment_id
                    result = await response.json()
                    attachment_id = result['attachment_id']
                    return attachment_id
            except:
                raise FacebookUploadError(f"Converting attachment {url} fail.")
        else:
            raise FacebookUploadError(f"No content type found for {extension} MIME type.")
    except:
        raise FacebookUploadError(f'File format {extension} not recognized.')


class FacebookUploadError(Exception):
    pass
