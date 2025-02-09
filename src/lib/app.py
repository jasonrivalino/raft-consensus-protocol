class Application:
    def __init__(self):
        self.data = {}

    def ping(self):
        return "PONG"

    def get(self, key):
        return self.data.get(key, "")

    def set(self, key, value):
        self.data[key] = value
        return "OK"

    def strln(self, key):
        value = self.data.get(key, "")
        return len(value)

    def delete(self, key):
        return self.data.pop(key, "")

    def append(self, key, value):
        if key in self.data:
            self.data[key] += value
        else:
            self.data[key] = value
        return "OK"
