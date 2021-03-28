import logging


logger = logging.getLogger(__name__)


class Weather:
    def __init__(self):
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        weather = message.value()
        self.temperature = weather['temperature']
        self.status = weather['status']