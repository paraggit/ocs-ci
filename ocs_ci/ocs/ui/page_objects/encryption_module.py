from ocs_ci.ocs.ui.helpers_ui import logger, extract_encryption_status
from ocs_ci.ocs.constants import ENCRYPTION_DASHBOARD_CONTEXT_MAP
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator


class EncryptionModule(PageNavigator):
    def _get_encryption_summary(self, context_key):
        """
        Generic method to collect encryption summary based on the context.

        Args:
            context_key (str): Key to determine the validation location.

        Returns:
            dict: Encryption summary for the given context.
        """
        encryption_summary = {
            "object_storage": {"status": None, "kms": ""},
            "cluster_wide_encryption": {"status": None, "kms": ""},
            "storageclass_encryption": {"status": None, "kms": ""},
            "intransit_encryption": {"status": None},
        }

        logger.info(f"Getting Encryption Summary for context: {context_key}")

        # Open the encryption summary popup
        self.do_click(
            self.validation_loc["encryption_summary"][context_key]["enabled"],
            enable_screenshot=True,
        )

        self.page_has_loaded(
            module_loc=self.validation_loc["encryption_summary"][context_key][
                "encryption_content_data"
            ]
        )

        # Get elements for text and root
        encryption_content_location = self.validation_loc["encryption_summary"][
            context_key
        ]["encryption_content_data"]
        encryption_summary_text = self.get_element_text(encryption_content_location)
        root_elements = self.get_elements(encryption_content_location)

        if not root_elements:
            raise ValueError("Error getting root web element")
        root_element = root_elements[0]

        # Process encryption summary text
        current_context = None
        for line in encryption_summary_text.split("\n"):
            line = line.strip()
            if line in ENCRYPTION_DASHBOARD_CONTEXT_MAP:
                current_context = ENCRYPTION_DASHBOARD_CONTEXT_MAP[line]
                continue

            if (
                current_context
                in [
                    "object_storage",
                    "cluster_wide_encryption",
                    "storageclass_encryption",
                ]
                and "External Key Management Service" in line
            ):
                encryption_summary[current_context]["kms"] = line.split(":")[-1].strip()
                encryption_summary[current_context]["status"] = (
                    extract_encryption_status(
                        root_element,
                        self._get_svg_selector(context_key, current_context),
                    )
                )
            elif current_context == "intransit_encryption":
                encryption_summary[current_context]["status"] = (
                    extract_encryption_status(
                        root_element,
                        self._get_svg_selector(context_key, current_context),
                    )
                )

        logger.info(f"Encryption Summary for {context_key}: {encryption_summary}")

        # Close the popup
        logger.info("Closing the popup")
        self.do_click(
            self.validation_loc["encryption_summary"][context_key]["close"],
            enable_screenshot=True,
        )

        return encryption_summary

    def _get_svg_selector(self, context_key, current_context):
        """
        Get the appropriate SVG selector for extracting encryption status.

        Args:
            context_key (str): The context key.
            current_context (str): The current encryption context.

        Returns:
            str: SVG selector path.
        """
        selectors = {
            "object_storage": {
                "object_storage": "div.pf-v5-l-flex:nth-child(1) > div:nth-child(2) > svg",
                "intransit_encryption": "div.pf-v5-l-flex:nth-child(4) > div:nth-child(2) > svg",
            },
            "file_and_block": {
                "cluster_wide_encryption": (
                    "div.pf-m-align-items-center:nth-child(1) > "
                    "div:nth-child(2) > svg:nth-child(1)"
                ),
                "storageclass_encryption": (
                    "div.pf-v5-l-flex:nth-child(6) > "
                    "div:nth-child(2) > svg:nth-child(1)"
                ),
                "intransit_encryption": "div.pf-v5-l-flex:nth-child(10) > div:nth-child(2) > svg",
            },
        }
        return selectors.get(context_key, {}).get(current_context, "")

    def get_object_encryption_summary(self):
        """
        Retrieve the encryption summary for the object details page.

        Returns:
            dict: Encryption summary on object details page.
        """
        return self._get_encryption_summary("object_storage")

    def get_block_file_encryption_summary(self):
        """
        Retrieve the encryption summary for the block and file page.

        Returns:
            dict: Encryption summary on block and file page.
        """
        return self._get_encryption_summary("file_and_block")
