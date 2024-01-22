# application/octet-stream
from pathlib import Path
import mailparser
from mailparse import EmailDecode, EmailEncode
import json
import email
import io
import base64
import quopri
import unicodedata

def encode_email(file_bytes, filename):
    email_obj = EmailDecode.load(file_bytes)
    if email_obj['text'] is None and email_obj['html'] is None:
        email_obj['text'] = 'Empty Message Body'
    new_path = Path(filename).parent / Path(str(Path(filename).stem) + '.tmp')
    failed = False
    try:
        with open(new_path, 'wb') as w:
            w.write(bytes(EmailEncode(email_obj)))
    except UnicodeEncodeError as e:
        failed = True
    if failed:
        new_path.unlink()
    else:
        Path(filename).unlink()
        Path(new_path).rename(filename)
    return False if failed else True
    

def reencode_attachments(py_email):
    if py_email.is_multipart():
        #print('multipart')
        parts = list(py_email.walk())
        for part in parts:
            ctype = part.get_content_type()

            cdispo = str(part.get('Content-Disposition'))

            if 'attachment' in cdispo or 'inline' in cdispo:
                #print('attachment')
                attachment_bytes = io.BytesIO(part.get_payload(decode=True))
                part.set_payload(attachment_bytes.read(), 'latin1')
                del(part['Content-Transfer-Encoding'])
                email.encoders.encode_base64(part)
    return py_email

def reencode_email(filename):
    if Path(filename).suffix == '.eml':
        return None
    file_bytes = b''
    with open(filename, 'rb') as f:
        file_bytes = f.read()
    py_email = email.message_from_bytes(file_bytes, policy=email.policy.default)
    new_py_email = reencode_attachments(py_email)
    try:
        encoded = encode_email(new_py_email.as_bytes(), filename)
        return 'reencoded' if encoded is True else 'renamed'
    except UnicodeEncodeError:
        return 'renamed'

if __name__ == '__main__':
    result = reencode_email('/home/cameron/Documents/New Folder/extracted/Emails_203/878/4beefe1e-59d3-42d4-a2e2-c463830789bf/be1fea54-d107-403f-ab1d-7cad80aac6f5')
    print(result)