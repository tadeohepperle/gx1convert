import gx1convert
print("Hello World")
res = gx1convert.gx1_to_parquet(
    '../V2_00001.dat', '../V2_00001.hdr', 'out.parquet')

print("Hello World 2")
# print(type(res))
# print(res)
