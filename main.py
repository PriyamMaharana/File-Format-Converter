from converter import convert_file

input_path = input("Enter or paste the path to the file you want to convert: ")

print(f'Available formats: CSV, JSON, XLS, XlSX')

output_format = input("Enter desire output format: ")

convert_file(input_path, output_format)