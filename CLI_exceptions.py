class DuplicateIDError(Exception):
    def __init__(self, duplicate_id_list):
        self.duplicate_id_list = duplicate_id_list