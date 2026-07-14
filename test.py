from datetime import datetime

from vnstock_data import Reference

ref = Reference()

# # Tất cả symbol
# all_symbols = ref.equity.list()
# print(all_symbols)
#
# # Cổ phiếu nhóm VN30
# vn30 = ref.equity.list_by_group("VN30")
# print(vn30)

# Cổ phiếu sàn HSX
hsx_stocks = ref.equity.list_by_exchange('HSX')
print(hsx_stocks)

# # Cổ phiếu theo ngành ICB
# industry_stocks = ref.equity.list_by_industry()
# print(industry_stocks)