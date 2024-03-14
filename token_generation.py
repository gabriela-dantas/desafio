import secrets

generated_secret = secrets.token_urlsafe(32)
print(generated_secret)
