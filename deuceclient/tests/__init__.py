"""
Deuce Client Tests
"""
import datetime
import uuid

import deuceclient.auth.base


class FakeAuthenticator(deuceclient.auth.base.AuthenticationBase):

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.__tenantid = "tid_{0}".format(str(uuid.uuid4()))

        self.__token_data = {}
        self.__token_data['token'] = None
        self.__token_data['expires'] = datetime.datetime.utcnow()
        self.__token_data['lifetime_seconds'] = 3600  # 1 Hour

    @staticmethod
    def MakeToken(expires_in_seconds):
        expires_at = (datetime.datetime.utcnow() +
                      datetime.timedelta(seconds=expires_in_seconds))
        token = "token_{0}".format(str(uuid.uuid4()))
        return (expires_at, token)

    def GetToken(self, retry=5):
        expires, token = FakeAuthenticator.MakeToken(
            self.__token_data['lifetime_seconds'])
        if expires <= datetime.datetime.utcnow():
            if retry:
                return self.GetToken(retry - 1)
            else:
                return None
        else:
            self.__token_data['token'] = token
            self.__token_data['expires'] = expires_at
            return token

    def IsExpired(self, fuzz=0):
        test_time = (self.AuthExpirationTime() +
                    datetime.timedelta(seconds=fuzz))
        return (test_time >= datetime.datetime.utcnow())

    def AuthExpirationTime(self):
        return self.__token_data['expires']

    def _AuthToken(self):
        if self.IsExpired():
            return self.GetToken()

        elif self.IsExpired(fuzz=2):
            time.sleep(3)
            return self.GetToken()

        else:
            return self.__access.auth_token

    def _AuthTenantId(self):
        return self.__tenantid
