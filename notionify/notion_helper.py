import logging
import os

from dotenv import load_dotenv
from notion_client import Client
from retrying import retry

from notionify.notion_utils import extract_page_id

load_dotenv()


class NotionHelper:
    database_id_dict = {}
    heatmap_block_id = None

    def __init__(self):
        self.client = Client(auth=os.getenv("NOTION_TOKEN"), log_level=logging.ERROR)
        self.page_id = extract_page_id(os.getenv("NOTION_PAGE"))
        self.__cache = {}

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def clear_page_content(self, page_id):
        result = self.client.blocks.children.list(block_id=page_id)
        if not result:
            return

        blocks = result.get('results', [])

        for block in blocks:
            block_id = block['id']
            self.client.blocks.delete(block_id=block_id)

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def update_book_page(self, page_id, properties):
        return self.client.pages.update(page_id=page_id, properties=properties)

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def update_page(self, page_id, properties, cover):
        return self.client.pages.update(
            page_id=page_id, properties=properties, cover=cover
        )

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def create_page(self, parent, properties, icon):
        return self.client.pages.create(parent=parent, properties=properties, icon=icon)

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def create_book_page(self, parent, properties, icon):
        return self.client.pages.create(
            parent=parent, properties=properties, icon=icon, cover=icon
        )

    # ---------- FIXED FOR NOTION SDK V2 ----------
    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def query(self, database_id=None, **kwargs):
        """
        Wraps notion.databases.query
        Must include database_id in Notion SDK v2
        """
        if not database_id:
            raise ValueError("database_id is required for Notion SDK v2")

        kwargs = {k: v for k, v in kwargs.items() if v}
        return self.client.databases.query(database_id=database_id, **kwargs)

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def get_block_children(self, id):
        response = self.client.blocks.children.list(block_id=id)
        return response.get("results", [])

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def append_blocks(self, block_id, children):
        return self.client.blocks.children.append(block_id=block_id, children=children)

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def append_blocks_after(self, block_id, children, after):
        return self.client.blocks.children.append(
            block_id=block_id, children=children, after=after
        )

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def delete_block(self, block_id):
        return self.client.blocks.delete(block_id=block_id)

    # ---------- FIXED FOR NOTION SDK V2 ----------
    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def query_all(self, database_id):
        """Retrieve all rows from a Notion database (v2 compatible)."""
        results = []
        has_more = True
        start_cursor = None

        while has_more:
            response = self.client.databases.query(
                database_id=database_id,
                start_cursor=start_cursor,
                page_size=100,
            )
            results.extend(response.get("results", []))
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")

        return results


if __name__ == "__main__":
    notion_helper = NotionHelper()
    print(notion_helper.query_all(database_id="test"))
