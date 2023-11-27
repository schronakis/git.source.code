############################# importing required modules ####################################
from pathlib import Path
from misc_code.db_connect import db_connect
import pdfplumber
import json
##############################################################################################

################################## db_connection_pdf_path ####################################

db_conn = db_connect('SQL Server', 'E352012', 'MyWork', 'yes')

db_conn_cursor = db_conn.initiate_connection()

db_conn_cursor.execute("TRUNCATE TABLE stg.pdf_enimerotiko")

pdf_path = (
     Path.home()
     / r"C:\\Users\\schronakis\\Downloads\\enel\\"
     / "ΔΑΠΕΕΠ_Ενημερωτικό Σημείωμα.pdf") # 20230101_20230131_15minAverageFRCE_01 ERP1_APP_8000142549_20230925_998384349_10017 ΔΑΠΕΕΠ_Ενημερωτικό Σημείωμα
##############################################################################################

########################################## pdfPlumper ########################################
columns = ('col_1','col_2','col_3','col_4','col_5','col_6','col_7','col_8')
columns_joined = ', '.join(columns)

with pdfplumber.open(pdf_path) as pdf:
    # Extract the data
    for page in pdf.pages:
        text = page.extract_table() # or extract_tables  extract_text
        for enum_index, i in enumerate(text):
            if all(v is not (None or '') for v in i):
                print(f'row_{enum_index}: {i[0]}: {i[1]}')
                json_string = json.dumps(i,ensure_ascii=False)[1:-1].replace('"','').replace('\\n', ' ').replace("πληρωμής,","πληρωμής")
                print(json_string)
#                 i_tuple = (tuple(map(str, json_string.split(', '))))
#                 db_conn_cursor.execute('INSERT INTO stg.pdf_enimerotiko(' + columns_joined + ') VALUES (' + ','.join('?' * len(i_tuple)) + ')' ,i_tuple)

# db_conn.db_commit()
# db_conn.end_connection()

    # # Extract the images
    # images = pdf.get_images()
    # for image in images:
    #     print(image["page_number"])
    #     with open(f"image_{image['page_number']}.jpg", "wb") as f:
    #         f.write(image["data"])
############################################################################################## 

########################################### PDFQuery #########################################
# from pdfquery import PDFQuery

# pdf = PDFQuery(pdf_path)
# pdf.load()
# # pdf.tree.write('C:\\admie\\ΔΑΠΕΕΠ_Ενημερωτικό_Σημείωμα.xml', pretty_print=True, encoding="utf-8")

# get_month = pdf.pq('LTTextLineHorizontal:in_bbox("231.08, 612.177, 333.32, 624.177")').text()
# print(get_month)

# # Use CSS-like selectors to locate the elements
# text_elements = pdf.pq('LTTextLineHorizontal')
# get_name = pdf.pq('LTTextLineHorizontal:contains("ΑΕΡΓΟΣ")')[0]
# print(text_elements)

# # Extract the text from the elements
# text = [t.text for t in get_name]
# print(text)


# x = float(get_name.get('x0'))
# y = float(get_name.get('y0'))
# cells = pdf.extract( [
#          ('with_parent','LTPage[pageid=\'1\']'),
#          ('cells', 'LTTextLineHorizontal:in_bbox("%s,%s,%s,%s")' % (x, y-500, x+150, y))])
# print([cell.text.encode('utf-8').strip() for cell in cells['cells']])

##############################################################################################


######################## pdfminer:get_pdf_coordinates ########################################
# from pdfminer.layout import LAParams, LTTextBox
# from pdfminer.pdfpage import PDFPage
# from pdfminer.pdfinterp import PDFResourceManager
# from pdfminer.pdfinterp import PDFPageInterpreter
# from pdfminer.converter import PDFPageAggregator

# fp = open(pdf_path, 'rb')
# rsrcmgr = PDFResourceManager()
# laparams = LAParams()
# device = PDFPageAggregator(rsrcmgr, laparams=laparams)
# interpreter = PDFPageInterpreter(rsrcmgr, device)
# pages = PDFPage.get_pages(fp)

# for page in pages:
#     # print('Processing next page...')
#     interpreter.process_page(page)
#     layout = device.get_result()
#     for lobj in layout:
#         if isinstance(lobj, LTTextBox):
#             x, y, text = lobj.bbox[0], lobj.bbox[3], lobj.get_text()
#             print('At %r is text: %s' % ((x, y), text))
##############################################################################################
