class Error(Exception):
    """Base class for errors
       expression - input expression in which error occurred
       message - explanation of error
    """
    def __init__(self, message: str) -> None:
        self.message = "ERROR : "+message

    def log(self):
        print(self.str())

class TaskTypeError(Error):
    """Error for incorrect task type
    """
    def __init__(self, task_type: str) -> None:
        super()
        self.message = task_type
