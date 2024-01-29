import pandas as pd
import os
import re
import json

file_name_header_absent = []
new_header_data = []
file_name_data = []
header_count = []
po_number = []
sales_order_number=[]
invoice_number = []
file_name_invoiceDate_absent = []
file_name_invoiceDate_wrong_dt = []
file_name_items_absent = []
file_name_data_items = []
new_item_data = []
file_item_count = []
file_absent = []
product_name = []
product_code = []
product_Desc = []
line_number = []
ordered_qty = []
backOrdered_qty = []
shipped_qty = []
amount = []
unit_price_errors = []

#read_json 
json_file = pd.read_json(path_or_buf=r"C:\Users\DELL\Desktop\dataset-48\validation\metadata.jsonl", lines=True)
ground_truth = json_file.get('ground_truth').to_list()
full_file_name_data = json_file.get('file_name').to_list()

#function for removing gt_parse
def process_ground_truth(ground_truth, pattern):
    result_list = []
    for index, item in enumerate(ground_truth):
        str1 = ""
        for i in range(len(pattern) - 1, len(item) - 1):
            str1 += item[i]
        result_list.append(str1)
    return result_list
    
#processing header and returns modifies header_data list
def processing_header(result):
    new_header = []
    new_file_data = []
    header_absent = []
    header_data = []
    # Populate header_data
    for i in result:
        new_json_file = json.loads(i)
        header_data.append(new_json_file.get('header'))
    new_header = header_data.copy()
    # Identify indices to remove
    indices_to_remove = [index for index, item in enumerate(header_data) if item is None]
    for index, item in enumerate(full_file_name_data):
        if index in indices_to_remove:
            header_absent.append(item)
        else:
            new_file_data.append(item)

    new_header_data = header_data.copy()
    for index in reversed(indices_to_remove):
        new_header.pop(index)
    return new_header , new_file_data , header_absent

#check the count of header and write into csv

def header_count(headerData , file_name_data):
    header_key_count = []
    for index_number , data in enumerate(headerData):
        len_data = len(data)
        if len_data == 3 or len_data == 4:
            pass
        else:
            header_key_count.append(file_name_data[index_number])
        
       
    df = pd.DataFrame({'File Name Absent': header_key_count })
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_folder = os.path.join(desktop_path, "output")
    os.makedirs(output_folder, exist_ok=True)
    df.to_csv(os.path.join(output_folder  , 'header_count.csv'), index = False)

            
#check the label invoice Date and write into csv
def process_invoice_date_errors(new_header_data, file_name_data):
    file_name_invoiceDate_absent = []
    file_name_invoiceDate_wrong_dt = []

    pattern_1_for_invoice_date = re.compile(r'\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})\b|\d[0-9]{0,9}')

    for index_number, individual_dict in enumerate(new_header_data):
        if 'invoiceDate' in individual_dict:
            invoice_date = individual_dict.get('invoiceDate')
            try:
                if isinstance(invoice_date, str): 
                    if re.match(pattern_1_for_invoice_date, invoice_date):
                        pass
                    else:
                        file_name_invoiceDate_wrong_dt.append(file_name_data[index_number])
            except Exception as e:
                print(f"Error processing invoiceDate: {e}")
        else:
            file_name_invoiceDate_absent.append(file_name_data[index_number])

    # Make lengths of both lists equal
    max_length = max(len(file_name_invoiceDate_absent), len(file_name_invoiceDate_wrong_dt))
    file_name_invoiceDate_absent += ['' for _ in range(max_length - len(file_name_invoiceDate_absent))]
    file_name_invoiceDate_wrong_dt += ['' for _ in range(max_length - len(file_name_invoiceDate_wrong_dt))]

    # Combine the two lists into a DataFrame
    df = pd.DataFrame({'File Name Absent': file_name_invoiceDate_absent, 'File Name Wrong DT': file_name_invoiceDate_wrong_dt})

    # Get the desktop path
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_folder = os.path.join(desktop_path, "output")
    os.makedirs(output_folder, exist_ok=True)

    # Save DataFrame to CSV
    csv_file_path = os.path.join(output_folder, "combined_invoice_date_errors.csv")
    df.to_csv(csv_file_path, index=False)

#function to store the label data into respective lists
def header_labels(label):
    labels = []
    for index_header_data , individual_header_items in enumerate(new_header_data):
        individual_labels = individual_header_items.get(label)
        labels.append(individual_labels)
    return labels
    

#function to write header labels bad files into csv
def save_files_absent_to_csv(values, file_names, label_name):
    # Identify indices with None values
    indices_none = [index for index, value in enumerate(values) if value is None]

    # Ensure indices are within the range of file_names
    valid_indices = [index for index in indices_none if index < len(file_names)]

    # Extract corresponding file names
    file_names_absent = [file_names[index] for index in valid_indices]

    # Create a DataFrame with a single column 'File Name'
    df = pd.DataFrame({'File Name Absent': file_names_absent})

    # Get the desktop path
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_folder = os.path.join(desktop_path, "output")
    os.makedirs(output_folder, exist_ok=True)

    # Save DataFrame to CSV
    csv_file_path = os.path.join(output_folder, '{}.csv'.format(label_name.lower()))
    df.to_csv(csv_file_path, index=False)


#Now processing Items data 
def processing_items(result):
    items_data = []
    new_item = []
    for i in result:
        new_json_file = json.loads(i)
        items_data.append(new_json_file.get('items'))
        
    file_name_items_absent = []
    file_name = []

    # Identify indices to remove
    indices_to_remove = [index for index, item in enumerate(items_data) if item is None]

    # Populate the file_name_not_item_data_present list and remove unnecessary elements
    for index, item in enumerate(full_file_name_data):
        if index in indices_to_remove:
            file_name_items_absent.append(item)
        else:
            file_name.append(item)

    # Update list_indexes
    new_item = items_data.copy()
    for index in reversed(indices_to_remove):
        new_item.pop(index)
    return new_item , file_name , file_name_items_absent
    
#check the count of items and write into csv
def process_item_count_errors(new_item_data, file_name_data_items):
    file_absent = []

    for index_number, item in enumerate(new_item_data):
        line_number = []
        file_name_printed = False

        for i, data in enumerate(item):
            len_data = len(data)
            
            if len_data == 6 or len_data == 7 or len_data == 8:
                pass
            else:
                if not file_name_printed:
                    file_item_count = file_name_data_items[index_number]
                    file_name_printed = True
                line_number.append(i)
        
        if line_number:
            file_absent.append({'file Name': file_item_count, 'Line Number': line_number})

    df = pd.DataFrame(file_absent, columns=['file Name', 'Line Number'])

    # Get the desktop path
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_folder = os.path.join(desktop_path, "output")
    os.makedirs(output_folder, exist_ok=True)

    # Save DataFrame to CSV
    csv_file_path = os.path.join(output_folder, "item_count.csv")
    df.to_csv(csv_file_path, index=False)


#function for product_code , productDesc , productName and lineNumber because they only checked their presence 
def errorCheckerInItemsLabel(label):
    file_names = []  
    list_contain_fileNames_and_lineNumbers = []
    
    for index_numbers, individual_list in enumerate(new_item_data):
        file_name_printed = False
        line_number = []
        
        for index, individual_dict in enumerate(individual_list):
            if label in individual_dict:
                pass
            else:
                if not file_name_printed:
                    file_names = file_name_data_items[index_numbers]
                    file_name_printed = True
                line_number.append(index)
        
        if line_number:
            list_contain_fileNames_and_lineNumbers.append({'file name Absent': file_names, 'line number Absent': line_number})
    
    return list_contain_fileNames_and_lineNumbers


#function for orderedQuantity , backOrderedQuantity , shippedQuantity ,amount and unitPrice  because they checked their presence as well as their datatype
def errorCheckerInRemainingItemsLabel(label):
    file_name_absent = []
    file_name_wrong_dt = []
    bad_files_not_present = []
    bad_files_wrong_dt = []

    for index_number, individual_list in enumerate(new_item_data):
        
        file_name_printed = False
        line_number_absent = [] 
        line_number_wrong_dt = []
        for index, individual_dict in enumerate(individual_list):
            
            if label in individual_dict:
                label_qty = individual_dict.get(label)
                if isinstance(label_qty, str):
                    try:
                        cleaned_qty = label_qty.replace(',' , '')
                        cleaned_qty = cleaned_qty.rstrip('T')
                        float(cleaned_qty)
                    except ValueError:
                        if not file_name_printed:
                            ind = index
                            file_name_wrong_dt = file_name_data_items[index_number]
                            file_name_printed = True
                        line_number_wrong_dt.append(index)
                
            else:
                if not file_name_printed:
                    file_name_absent = file_name_data[index_number]
                    file_name_printed = True
                line_number_absent.append(index)

        if line_number_wrong_dt:
            bad_files_wrong_dt.append({'file name For wrong dt': file_name_wrong_dt, 'line number for wrong dt': line_number_wrong_dt})
        if line_number_absent:
            bad_files_not_present.append({'file Name Absent': file_name_absent, 'line Number Absent': line_number_absent})
            
    return bad_files_not_present, bad_files_wrong_dt

       
#function for the labels that only constitute checking of their presence
def write_errors_to_csv(errors_list, label_name):
    # Combine the dictionaries into a DataFrame
    error_df = pd.DataFrame({
        'File Name Absent': [item.get('file name Absent', '') for item in errors_list],
        'Line Number Absent': [item.get('line number Absent', '') for item in errors_list],
    })

    # Get the desktop path
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_folder = os.path.join(desktop_path, "output")
    os.makedirs(output_folder, exist_ok=True)

    # Save DataFrame to CSV
    csv_file_path = os.path.join(output_folder, f"{label_name.lower()}_errors.csv")
    error_df.to_csv(csv_file_path, index=False)

#function for the labels that constitute checking for their presence as well as their datatype
def save_errors_to_csv(errors_absent, errors_wrong_dt, label_name):
    # Make lengths of both lists equal
    max_length = max(len(errors_absent), len(errors_wrong_dt))
    errors_absent += [{} for _ in range(max_length - len(errors_absent))]
    errors_wrong_dt += [{} for _ in range(max_length - len(errors_wrong_dt))]

    # Combine the two lists into a DataFrame
    error_df = pd.DataFrame({
        'File Name Absent': [item.get('file Name Absent', '') if 'file Name Absent' in item else '' for item in errors_absent],
        'Line Number Absent': [item.get('line Number Absent', '') if 'line Number Absent' in item else '' for item in errors_absent],
        'File name For wrong dt': [item.get('file name For wrong dt', '') for item in errors_wrong_dt],
        'Line number for wrong dt': [item.get('line number for wrong dt', '') for item in errors_wrong_dt],
    })

    # Get the desktop path
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_folder = os.path.join(desktop_path, "output")
    os.makedirs(output_folder, exist_ok=True)

    # Save DataFrame to CSV
    csv_file_path = os.path.join(output_folder, f"{label_name.lower()}_errors.csv")
    error_df.to_csv(csv_file_path, index=False)
 
#function for combining all the csv's
def combined_csv():


    file_names = []
    
    home = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_path = os.path.join(home, 'output')

    # Install openpyxl if not installed
    try:
        import openpyxl
    except ImportError:
        os.system('pip install openpyxl')

    with pd.ExcelWriter(os.path.join(output_path, 'combined_xlsx.xlsx'), engine='openpyxl') as writer:
        for file in os.listdir(output_path):
            if file.endswith('.csv'):
                temp = file.replace('.csv', '')
                json_load = pd.read_csv(os.path.join(output_path, file))
                json_load.to_excel(writer, sheet_name=temp, index=False)
                
if __name__ == '__main__':
    pattern = r'\{"gt_parse": '  # Pattern which we want to remove
    result = process_ground_truth(ground_truth, pattern)
    
    new_header_data, file_name_data, file_name_header_absent = processing_header(result)
    
    header_count(new_header_data , file_name_data)
    process_invoice_date_errors(new_header_data, file_name_data)
    
    po_number = header_labels('poNumber')
    sales_order_number=header_labels('salesOrderNumber')
    invoice_number = header_labels('invoiceNumber')
    
    save_files_absent_to_csv(invoice_number, file_name_data, 'InvoiceNumber')
    save_files_absent_to_csv(sales_order_number, file_name_data, 'salesOrderNumber')
    save_files_absent_to_csv(po_number, file_name_data, 'poNumber')
    
    new_item_data , file_name_data_items , file_name_items_absent = processing_items(result)
    
    process_item_count_errors(new_item_data, file_name_data_items)
    
    product_name = errorCheckerInItemsLabel('productName')
    product_code = errorCheckerInItemsLabel('productCode')
    product_Desc = errorCheckerInItemsLabel('productDesc')
    line_number = errorCheckerInItemsLabel('lineNumber')

    write_errors_to_csv(product_Desc, 'ProductDesc')
    write_errors_to_csv(product_code, 'ProductCode')
    write_errors_to_csv(product_name ,  'ProductName')
    write_errors_to_csv(line_number, 'lineNumber')
    
    ordered_qty = errorCheckerInRemainingItemsLabel('orderedQuantity') 
    backOrdered_qty = errorCheckerInRemainingItemsLabel('backOrderedQuantity') 
    shipped_qty = errorCheckerInRemainingItemsLabel('shippedQuantity')    
    amount = errorCheckerInRemainingItemsLabel('amount')
    unit_price_errors = errorCheckerInRemainingItemsLabel('unitPrice')
     
    save_errors_to_csv(ordered_qty[0], ordered_qty[1], 'orderedQuantity') 
    save_errors_to_csv(backOrdered_qty[0], backOrdered_qty[1], 'backOrderedQuantity') 
    save_errors_to_csv(shipped_qty[0], shipped_qty[1], 'shippedQuantity') 
    save_errors_to_csv(amount[0], amount[1], 'amount') 
    save_errors_to_csv(unit_price_errors[0], unit_price_errors[1], 'unitPrice') 
                
    combined_csv()
        