"""
Function mapping module that provides a centralized dictionary of all processing functions.
"""

from .stock_exchange_related_functions import (
    extract_stock_exchange,
    get_year_code_from_context,
    get_product_code_from_context
)
from .NIC_code_related_functions import (
    extract_nic_group_code,
    extract_nic_class_code,
    extract_nic_subclass_code
)
from .context_utils import (
    get_benefit_code_from_context
)
from .activity_utils import extract_activity_code
from .reporting_boundary_related_functions import extract_reporting_boundary
from .address_utils import get_state_code

# Dictionary mapping function names to their implementations
function_map = {
    "extract_nic_group_code": extract_nic_group_code,
    "extract_nic_class_code": extract_nic_class_code,
    "extract_nic_subclass_code": extract_nic_subclass_code,
    "extract_activity_code": extract_activity_code,
    "extract_stock_exchange": extract_stock_exchange,
    "extract_reporting_boundary": extract_reporting_boundary,
    "get_product_code_from_context": get_product_code_from_context,
    "get_year_code_from_context": get_year_code_from_context,
    "get_benefit_code_from_context": get_benefit_code_from_context,
    "get_state_code": get_state_code
} 