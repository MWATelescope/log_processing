class HandlerBase():
    """
    Base handler class, provides the skip method and startup/shutdown methods.
    """
    def startup(self):
        """
        Will be ran at the start of the process, can be used for setup.
        """
        pass

    def shutdown(self):
        """
        Will be ran at the end of the process, can be used for cleanup.
        """
        pass

    def skip(self, *args):
        """
        Method used to skip a line.
        """
        return