"""Device code auth flow to get a new refresh token."""
import requests, time

CLIENT_ID = '14d82eec-204b-4c2f-b7e8-296a70dab67e'
SCOPE = 'Mail.Read Files.Read.All Sites.Read.All offline_access'

r = requests.post('https://login.microsoftonline.com/common/oauth2/v2.0/devicecode', data={
    'client_id': CLIENT_ID, 'scope': SCOPE,
})
data = r.json()
print('=' * 50)
print(f'Go to: {data["verification_uri"]}')
print(f'Enter code: {data["user_code"]}')
print('Sign in with: tylerk@timironmp.com')
print('=' * 50)

device_code = data['device_code']
interval = data.get('interval', 5)
print('Waiting for sign-in...')
for i in range(120):
    time.sleep(interval)
    tr = requests.post('https://login.microsoftonline.com/common/oauth2/v2.0/token', data={
        'client_id': CLIENT_ID,
        'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
        'device_code': device_code,
    })
    td = tr.json()
    if 'access_token' in td:
        print('\nSUCCESS!')
        with open('_new_refresh_token.txt', 'w') as f:
            f.write(td['refresh_token'])
        print(f'Saved to _new_refresh_token.txt ({len(td["refresh_token"])} chars)')
        break
    elif td.get('error') == 'authorization_pending':
        print('.', end='', flush=True)
    else:
        print(f'\nError: {td.get("error")}: {td.get("error_description", "")[:200]}')
        break
