import csv

input_file = '/mnt/c/Docker_POC/cleaned_flipkart_com-ecommerce_sample.csv'
output_file = '/mnt/c/Docker_POC/fixed_flipkart_com-ecommerce_sample.csv'

expected_columns = 15  

def clean_csv(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        headers = next(reader) 
        writer.writerow(headers)
        
        for row in reader:
            if len(row) > expected_columns:
                row = row[:expected_columns]  
            elif len(row) < expected_columns:
                row.extend([''] * (expected_columns - len(row)))  
            writer.writerow(row)

clean_csv(input_file, output_file)
