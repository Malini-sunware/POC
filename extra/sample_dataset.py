import csv

input_file = '/mnt/c/Docker_POC/fixed_flipkart_com-ecommerce_sample.csv'
output_file = '/mnt/c/Docker_POC/small_flipkart_com-ecommerce_sample.csv'

def create_smaller_csv(input_file, output_file, num_rows):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        headers = next(reader)  
        writer.writerow(headers)
        
        for i, row in enumerate(reader):
            if i < num_rows:
                writer.writerow(row)
            else:
                break

create_smaller_csv(input_file, output_file, 10) 
