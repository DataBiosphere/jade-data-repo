class LogClass:
    def __init__(self):
        self._time = None
        self.src_ip = None
        self.dest_ip = None
        self.dest_port = None
        self.user_name = None
        self.user_id = None
        self.user_id_provider = "Terra"
        self.session_id = None
        self.url = None
        self.app = "TDR"
        self.http_user_agent = None
        self.status = None
        self.http_content_type = None
        self.bytes = None
        self.duration = None
        self.nih_ico = None
        self.cadr_name = None
        self.user_country_name = None
        self.user_org = None
        self.user_email = None
        self.associated_study = None
        self.eRA_commons_id = "N/A"
        self.user_permission_group = None
        self.event_type = None
        self.dataset_id = None
        self.snapshot_id = None
        self.method = None

    def __str__(self):
        return f"{self._time}, {self.src_ip}, {self.dest_ip}, {self.dest_port}, {self.user_name}, {self.user_id}, {self.user_id_provider}, {self.session_id}, {self.url}, {self.app}, {self.http_user_agent}, {self.status}, {self.http_content_type}, {self.bytes}, {self.duration}, {self.nih_ico}, {self.cadr_name}, {self.user_country_name}, {self.user_org}, {self.user_email}, {self.associated_study}, {self.eRA_commons_id}, {self.user_permission_group}, {self.event_type}"
