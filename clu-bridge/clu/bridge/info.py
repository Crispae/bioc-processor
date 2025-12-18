from typing import List, Text


class AppInfo:
    """
    General information about the application.

    ***This repo was generated from a cookiecutter template published by myedibleenso and zwellington.
    See https://github.com/clu-ling/clu-template for more info.
    """

    version: Text = "0.1"
    description: Text = "Utilities for converting documents"

    authors: List[Text] = ["myedibleenso"]
    contact: Text = "gus@parsertongue.org"
    repo: Text = "https://github.com/clu-ling/clu-bridge"
    license: Text = "Apache 2.0"

    @property
    def download_url(self) -> str:
        return f"{self.repo}/archive/v{self.version}.zip"


info = AppInfo()
