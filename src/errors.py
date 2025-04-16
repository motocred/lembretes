# errors.py
class PhoneFormatError(Exception):
    """ Class error for bad formating in phone """
    def __init__(self, message: str="Invalid phone number format") -> None:
        self.message = message
        super().__init__(f"{self.message}")
