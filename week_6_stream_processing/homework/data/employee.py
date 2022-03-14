from datetime import datetime
import faust

class Employee(faust.Record, validation=True, date_parser=datetime.fromtimestamp):
    employee_id: int
    first_name: str
    last_name: str
    address: str
    date: datetime
    action: str