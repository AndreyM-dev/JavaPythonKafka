class Message(object):
    def __init__(self, id, message):
        self.id = id
        self.message = message

    def __str__(self):
        return f"Message: id: {self.id} , message: {self.message}"
