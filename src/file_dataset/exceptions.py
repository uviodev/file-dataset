"""Exception classes for file-dataset operations."""


class FileDatasetError(Exception):
    """Exception raised when file dataset operations fail.

    This exception contains a mapping of filenames to their specific error messages,
    allowing callers to understand which files failed and why.
    """

    def __init__(
        self,
        file_errors: dict[str, str],
        message: str = "File dataset operation failed",
    ) -> None:
        """Initialize FileDatasetError.

        Args:
            file_errors: Dictionary mapping filenames to error messages
            message: Overall error message
        """
        self.file_errors = file_errors
        super().__init__(message)

    def __str__(self) -> str:
        """Return string representation of the error."""
        error_details = ", ".join(
            f"{filename}: {error}" for filename, error in self.file_errors.items()
        )
        return f"{super().__str__()}. File errors: {error_details}"
