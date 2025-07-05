# debt_manager_app.py

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import gspread
from google.oauth2.service_account import Credentials
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.units import cm
from reportlab.lib import colors 
from io import BytesIO
import json # Import json module - Keeping this import as it's now explicitly used

# ---------------- Configuration ----------------
# สำคัญ: กรุณาเปลี่ยน ID นี้เป็น Spreadsheet ID ของ Google Sheet ของคุณเอง
# ตรวจสอบให้แน่ใจว่า GSHEET_URL นี้เป็น URL ของ Google Sheet ไฟล์เดียวที่คุณใช้
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1MUz_OOedJNyx9CynepFa0TUvy74ploJZEZ-LTBbirXw/edit?gid=0#gid=0"

# --- REQUIRED GOOGLE SHEET COLUMN NAMES (Exact Match) ---
# กรุณาตรวจสอบให้ **มั่นใจ 100%** ว่าหัวตาราง (แถวแรกสุด) ใน Google Sheet ของคุณ
# มีชื่อ **ตรงกันเป๊ะ** กับที่ระบุไว้ด้านล่างนี้ (คัดลอกและวางได้เลย)
# ไม่มีช่องว่างนำหน้า/ต่อท้าย หรืออักขระพิเศษอื่นใดที่มองไม่เห็น

# สำหรับชีต 'cus':
# NO
# NAME
# LOAN

# สำหรับชีต 'pay':
# ผู้จ่าย
# วันที่จ่าย
# จำนวน
# หมายเหตุ
# ---------------------------------------------------------

# ---------------- Google Sheets Helpers ----------------
@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        # โหลดข้อมูล Service Account จาก Streamlit Secrets
        # ต้องแน่ใจว่าได้ตั้งค่า secret ชื่อ "gcp_service_account" ใน Streamlit Cloud แล้ว
        # โดยมีเนื้อหาเป็น JSON object ทั้งหมดของ Service Account key
        creds_json_string = st.secrets["gcp_service_account"] # นี่คือข้อความ string
        creds_json = json.loads(creds_json_string) # *** เพิ่มบรรทัดนี้เพื่อแปลง string เป็น JSON object ***
        creds = Credentials.from_service_account_info(creds_json, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ **ข้อผิดพลาดในการเชื่อมต่อ Google Sheets**: {e}")
        st.info("โปรดตรวจสอบ:")
        st.markdown("- คุณได้ตั้งค่า Streamlit Secret ชื่อ `gcp_service_account` แล้วหรือยัง?")
        st.markdown("- เนื้อหาของ `gcp_service_account` Secret ถูกต้องสมบูรณ์หรือไม่? ควรเป็น JSON object ทั้งหมดของ Service Account key")
        st.markdown("- Service Account มีสิทธิ์ 'ผู้แก้ไข' (Editor) ใน Google Sheet ของคุณแล้วหรือยัง?")
        st.markdown("- **สำหรับ Streamlit Cloud**: ให้คัดลอกไฟล์ JSON ของ Service Account ทั้งหมด ไปวางใน Streamlit Secrets (ดูวิธีการตั้งค่าในเอกสาร Streamlit เรื่อง Secrets)")
        st.stop()

# Helper to normalize column names from Google Sheets
# This will strip invisible characters or extra spaces from raw headers
def _normalize_gsheet_col_name(col_name):
    if isinstance(col_name, str):
        return col_name.strip().replace('\xa0', ' ').replace('\ufeff', '')
    return str(col_name) # Ensure it's a string

def read_sheet_to_df(sheet_name):
    client = get_gspread_client()
    try:
        sheet = client.open_by_url(GSHEET_URL).worksheet(sheet_name)
        raw_data = sheet.get_all_values()
        
        if not raw_data:
            st.warning(f"⚠️ ชีต '{sheet_name}' ว่างเปล่า.")
            return pd.DataFrame()
            
        # Normalize headers before using them
        headers = [_normalize_gsheet_col_name(h) for h in raw_data[0]]
        df = pd.DataFrame(raw_data[1:], columns=headers)
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ ไม่พบชีตชื่อ '{sheet_name}' ใน Google Sheet กรุณาตรวจสอบชื่อชีตให้ถูกต้อง.")
        st.stop()
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาดในการอ่านข้อมูลจากชีต '{sheet_name}': {e}")
        st.stop()

def write_df_to_sheet(sheet_name, df):
    client = get_gspread_client()
    try:
        sheet = client.open_by_url(GSHEET_URL).worksheet(sheet_name)
        sheet.clear()
        if not df.empty:
            sheet.update([df.columns.values.tolist()] + df.values.tolist())
        else:
            sheet.update([df.columns.values.tolist()])
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาดในการเขียนข้อมูลลงชีต '{sheet_name}': {e}")
        st.stop()

# ---------------- Load / Save Functions ----------------
@st.cache_data(ttl=3600)
def load_data():
    loading_status = st.empty()
    loading_status.info("⌛ กำลังโหลดข้อมูลจาก Google Sheets... โปรดรอสักครู่")

    try:
        customers_df = read_sheet_to_df("cus") # <<< แก้ไขตรงนี้จาก "customers" เป็น "cus"
        payments_df = read_sheet_to_df("pay")

        # --- Rename columns from Google Sheet names to internal names ---
        customer_col_map = {
            "NO": "ลำดับที่", # <<< แก้ไขตรงนี้จาก "ลำดับที่" เป็น "NO"
            "NAME": "NAME", # <<< แก้ไขตรงนี้จาก "ชื่อ" เป็น "NAME"
            "LOAN": "AmountDue" # <<< แก้ไขตรงนี้จาก "รวมเงินกู้ทั้งหมด" เป็น "LOAN"
        }
        
        payment_col_map = {
            "ผู้จ่าย": "ชื่อลูกค้า", 
            "วันที่จ่าย": "วันที่จ่าย_str",
            "จำนวน": "จำนวนเงิน",
            "หมายเหตุ": "หมายเหตุ"
        }

        # Check for missing columns BEFORE renaming
        # Check if the actual columns from the sheet are in the map's keys for customers
        missing_customer_cols_in_df = [col for col in customer_col_map.keys() if col not in customers_df.columns]
        if missing_customer_cols_in_df:
            loading_status.error(f"⚠️ **ข้อผิดพลาด**: ไม่พบคอลัมน์ที่จำเป็นในชีต 'cus': {', '.join(missing_customer_cols_in_df)}")
            st.error("โปรดตรวจสอบว่าหัวตารางใน Google Sheet 'cus' มีชื่อตรงกันเป๊ะกับ: 'NO', 'NAME', 'LOAN'")
            st.stop()

        missing_payment_cols_in_df = [col for col in payment_col_map.keys() if col not in payments_df.columns]
        if missing_payment_cols_in_df:
            loading_status.error(f"⚠️ **ข้อผิดพลาด**: ไม่พบคอลัมน์ที่จำเป็นในชีต 'pay': {', '.join(missing_payment_cols_in_df)}")
            st.error("โปรดตรวจสอบว่าหัวตารางใน Google Sheet 'pay' มีชื่อตรงกันเป๊ะกับ: 'ผู้จ่าย', 'วันที่จ่าย', 'จำนวน', 'หมายเหตุ'")
            st.stop()

        # Perform the renaming
        customers_df.rename(columns=customer_col_map, inplace=True)
        payments_df.rename(columns=payment_col_map, inplace=True)
        
        # --- VERIFY RENAMED COLUMNS EXIST ---
        expected_customer_cols_after_rename = ["ลำดับที่", "NAME", "AmountDue"]
        for col in expected_customer_cols_after_rename:
            if col not in customers_df.columns:
                loading_status.error(f"❌ **ข้อผิดพลาด**: คอลัมน์ '{col}' ไม่พบในชีต 'cus' หลังจากเปลี่ยนชื่อ. "
                                      "โปรดตรวจสอบว่าหัวตารางใน Google Sheet 'cus' ตรงกับข้อกำหนด: 'NO', 'NAME', 'LOAN'")
                st.stop()
                
        expected_payment_cols_after_rename = ["ชื่อลูกค้า", "วันที่จ่าย_str", "จำนวนเงิน", "หมายเหตุ"]
        for col in expected_payment_cols_after_rename:
            if col not in payments_df.columns:
                loading_status.error(f"❌ **ข้อผิดพลาด**: คอลัมน์ '{col}' ไม่พบในชีต 'pay' หลังจากเปลี่ยนชื่อ. "
                                      "โปรดตรวจสอบว่าหัวตารางใน Google Sheet 'pay' ตรงกับข้อกำหนด: 'ผู้จ่าย', 'วันที่จ่าย', 'จำนวน', 'หมายเหตุ'")
                st.stop()
        # --- END VERIFY ---

        # --- Data Cleaning and Type Conversion ---
        
        # Clean 'AmountDue' column in customers_df
        if 'AmountDue' in customers_df.columns:
            # Remove any non-numeric characters (except dot for decimal)
            customers_df['AmountDue'] = customers_df['AmountDue'].astype(str).str.replace(r'[^\d.]', '', regex=True)
            customers_df['AmountDue'] = pd.to_numeric(customers_df['AmountDue'], errors='coerce').fillna(0)
        else:
            customers_df['AmountDue'] = 0.0 # Default if column somehow not found

        # Clean 'จำนวนเงิน' column in payments_df
        if 'จำนวนเงิน' in payments_df.columns:
            payments_df['จำนวนเงิน'] = payments_df['จำนวนเงิน'].astype(str).str.replace(r'[^\d.]', '', regex=True)
            payments_df['จำนวนเงิน'] = pd.to_numeric(payments_df['จำนวนเงิน'], errors='coerce').fillna(0)
        else:
            payments_df['จำนวนเงิน'] = 0.0 # Default if column somehow not found

        # Add 'ลำดับที่' if missing or re-index for customers_df
        if 'ลำดับที่' not in customers_df.columns or customers_df.empty:
            customers_df['ลำดับที่'] = range(1, len(customers_df) + 1)
        
        # Convert types for customers_df
        customers_df['ลำดับที่'] = pd.to_numeric(customers_df['ลำดับที่'], errors='coerce').fillna(0).astype(int)
        customers_df = customers_df.sort_values(by='ลำดับที่', ascending=True).reset_index(drop=True)
        customers_df['ลำดับที่'] = range(1, len(customers_df) + 1)

        # Convert types for payments_df
        if "วันที่จ่าย_str" in payments_df.columns and not payments_df["วันที่จ่าย_str"].empty:
            payments_df['วันที่จ่าย'] = pd.to_datetime(payments_df['วันที่จ่าย_str'], errors='coerce', dayfirst=False)
            
            invalid_dates_mask = payments_df['วันที่จ่าย'].isna()
            if invalid_dates_mask.any():
                payments_df.loc[invalid_dates_mask, 'วันที่จ่าย'] = pd.to_datetime(
                    payments_df.loc[invalid_dates_mask, 'วันที่จ่าย_str'], errors='coerce', dayfirst=True
                )
        else:
            payments_df['วันที่จ่าย'] = pd.Series(dtype='datetime64[ns]')

        payments_df = payments_df.dropna(subset=['วันที่จ่าย'])
        payments_df = payments_df.reset_index(drop=True).assign(temp_index=range(len(payments_df)))

        customer_amounts = dict(zip(customers_df["NAME"], customers_df["AmountDue"]))
        
        loading_status.success("✅ โหลดข้อมูลเสร็จสมบูรณ์!")
        return customers_df, payments_df, customer_amounts

    except Exception as e:
        loading_status.error(f"❌ เกิดข้อผิดพลาดในการโหลดข้อมูลจาก Google Sheets: {e}")
        st.info("โปรดตรวจสอบ Spreadsheet ID, URL, ชื่อชีต, ชื่อคอลัมน์ และสิทธิ์การเข้าถึงของ Service Account.")
        st.stop()
        return pd.DataFrame(), pd.DataFrame(), {}

def save_customers_df(df):
    if not df.empty:
        df['ลำดับที่'] = range(1, len(df) + 1)
    
    df_to_save = df[["ลำดับที่", "NAME", "AmountDue"]].copy()
    df_to_save.rename(columns={
        "ลำดับที่": "NO", # <<< แก้ไขตรงนี้จาก "ลำดับที่" เป็น "NO"
        "NAME": "NAME", # <<< แก้ไขตรงนี้จาก "ชื่อ" เป็น "NAME"
        "AmountDue": "LOAN" # <<< แก้ไขตรงนี้จาก "รวมเงินกู้ทั้งหมด" เป็น "LOAN"
    }, inplace=True)
    write_df_to_sheet("cus", df_to_save) # <<< แก้ไขตรงนี้จาก "customers" เป็น "cus"
    st.cache_data.clear()
    st.rerun()

def save_payments_df(df):
    df_save = df.copy()
    
    if 'วันที่จ่าย' in df_save.columns:
        df_save['วันที่จ่าย_str_save'] = df_save['วันที่จ่าย'].dt.strftime("%Y-%m-%d")
    else:
        df_save['วันที่จ่าย_str_save'] = ''
    
    df_save = df_save[['ชื่อลูกค้า', 'วันที่จ่าย_str_save', 'จำนวนเงิน', 'หมายเหตุ']].copy()
    df_save.rename(columns={
        "ชื่อลูกค้า": "ผู้จ่าย",
        "วันที่จ่าย_str_save": "วันที่จ่าย",
        "จำนวนเงิน": "จำนวน",
        "หมายเหตุ": "หมายเหตุ"
    }, inplace=True)
    write_df_to_sheet("pay", df_save)
    st.cache_data.clear()
    st.rerun()

def clear_cache_and_rerun():
    st.cache_data.clear()
    st.rerun()

# ========== HELPER FUNCTIONS FOR DEBT CALCULATION ==========
def get_debt_periods(start_year=2025):
    periods = []
    contract_start_date = date(2025, 4, 5) 

    for i in range(4): # 4 ปี
        period_start = date(contract_start_date.year + i, 4, 5)
        period_end = date(contract_start_date.year + i + 1, 3, 5) # ถึงวันที่ 5 มีนาคมของปีถัดไป
        periods.append((period_start, period_end))
    return periods

def calculate_yearly_summary(debtor_name, total_loan, payments_df):
    yearly_target = total_loan * 0.25
    periods = get_debt_periods()
    summary_data = []
    total_fine_all_years = 0
    total_paid_all_years = 0
    current_date = datetime.now().date()

    for idx, (start_date, end_date) in enumerate(periods):
        year_label = f"ปีที่ {idx+1} ({start_date.year}/{end_date.year})"
        
        mask = (
            (payments_df['ชื่อลูกค้า'] == debtor_name) & 
            (payments_df['วันที่จ่าย'].dt.date >= start_date) &
            (payments_df['วันที่จ่าย'].dt.date <= end_date)
        )
        paid_in_period = payments_df.loc[mask, 'จำนวนเงิน'].sum() 
        total_paid_all_years += paid_in_period
        
        shortfall = max(0, yearly_target - paid_in_period) 
        fine = 0
        fine_status = ""
        fine_deadline = end_date 

        if current_date > fine_deadline:
            if shortfall > 0:
                fine = round(shortfall * 0.15) 
                total_fine_all_years += fine
                fine_status = f"❌ มีค่าปรับ **{fine:,.0f}** บาท"
            else:
                fine_status = "✅ จ่ายครบแล้ว"
        else:
            fine_status = "⏳ ยังไม่ครบกำหนดคิดค่าปรับ"

        summary_data.append({
            "ปี": year_label,
            "เป้าหมายรายปี": f"{yearly_target:,.0f} บาท",
            "จ่ายแล้วในปีนี้": f"{paid_in_period:,.0f} บาท",
            "ยอดขาด": f"{shortfall:,.0f} บาท",
            "สถานะค่าปรับ": fine_status
        })
    
    # Calculate remaining debt including fines
    remaining_original_debt = max(0, total_loan - total_paid_all_years)
    remaining_overall_debt_with_fine = remaining_original_debt + total_fine_all_years 

    return summary_data, total_paid_all_years, total_fine_all_years, remaining_overall_debt_with_fine

# ========== PDF GENERATION SETUP (GLOBAL SCOPE) ==========
@st.cache_resource
def setup_pdf_styles():
    thai_font_name_local = 'THSarabunNew'
    thai_font_name_bold_local = 'THSarabunNewBold'
    try:
        if 'THSarabunNew' not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont('THSarabunNew', 'THSarabunNew.ttf'))
        if 'THSarabunNewBold' not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont('THSarabunNewBold', 'THSarabunNew Bold.ttf'))
    except Exception as e:
        st.error(f"❌ **ข้อผิดพลาดในการโหลดฟอนต์ THSarabunNew**: ไม่พบไฟล์ 'THSarabunNew.ttf' หรือ 'THSarabunNew Bold.ttf' "
                 f"ในโฟลเดอร์เดียวกันกับ `debt_manager_app.py`. PDF อาจแสดงผลไม่ถูกต้อง. ({e})")
        thai_font_name_local = 'Helvetica'
        thai_font_name_bold_local = 'Helvetica-Bold'
        
    styles = getSampleStyleSheet()
    style_definitions = [
        ('TitleStyle', thai_font_name_bold_local, 28, TA_CENTER, 20), 
        ('Heading1', thai_font_name_bold_local, 20, TA_LEFT, 6),     
        ('Normal', thai_font_name_local, 14, TA_LEFT, 0),           
        ('SignatureCenter', thai_font_name_local, 14, TA_CENTER, 0), 
        ('SignatureLeft', thai_font_name_local, 14, TA_LEFT, 0),    
        ('SignatureRight', thai_font_name_local, 14, TA_RIGHT, 0),   
        ('RightAlign', thai_font_name_local, 14, TA_RIGHT, 0),      
        ('BoldNormal', thai_font_name_bold_local, 14, TA_LEFT, 0),  
        ('RightAlignAmount', thai_font_name_bold_local, 14, TA_RIGHT, 0),
        ('NormalLeft', thai_font_name_local, 14, TA_LEFT, 0) 
    ]
    for name, font, size, alignment, space_after in style_definitions:
        if name not in styles.byName: 
            styles.add(ParagraphStyle(
                name=name, 
                fontName=font, 
                fontSize=size, 
                alignment=alignment, 
                leading=size + 2, 
                spaceAfter=space_after
            ))
        else: 
            styles[name].fontName = font
            styles[name].fontSize = size
            styles[name].alignment = alignment
            styles[name].leading = size + 2
            styles[name].spaceAfter = space_after
    return styles, thai_font_name_local, thai_font_name_bold_local # Corrected to use _local variables

pdf_styles, thai_font_name, thai_font_name_bold = setup_pdf_styles()

# ========== PDF GENERATION FUNCTIONS ==========
def create_receipt_pdf(debtor_name, payment_date, amount_paid, 
                       yearly_summary_df, total_loan, total_paid_all_years, remaining_overall_debt_with_fine,
                       total_fine_all_years): 
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, 
                            leftMargin=2.5*cm, rightMargin=2.5*cm, 
                            topMargin=2.5*cm, bottomMargin=2.5*cm)
    elements = []

    elements.append(Paragraph("ใบเสร็จรับเงิน", pdf_styles['TitleStyle']))
    elements.append(Spacer(1, 0.8*cm)) 

    # ข้อมูลลูกหนี้และการชำระเงิน:
    data_info = [
        [Paragraph(f"ชื่อลูกหนี้: <font face='{thai_font_name_bold}'>{debtor_name}</font>", pdf_styles['NormalLeft']), ""],
        [Paragraph(f"วันที่ชำระ: {payment_date.strftime('%d/%m/%Y')}", pdf_styles['NormalLeft']), ""],
        [Paragraph(f"จำนวนเงินที่ชำระ:", pdf_styles['Normal']), Paragraph(f"<font face='{thai_font_name_bold}'>{amount_paid:,.2f} บาท</font>", pdf_styles['RightAlignAmount'])]
    ]
    table_info = Table(data_info, colWidths=[12*cm, 4*cm]) 
    table_info.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),  
        ('ALIGN', (1,2), (1,2), 'RIGHT'),  
        ('SPAN', (0,0), (1,0)), 
        ('SPAN', (0,1), (1,1)), 
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5), 
    ]))
    elements.append(table_info)
    
    elements.append(Spacer(1, 1.5*cm)) 
    elements.append(Paragraph("---", pdf_styles['Normal']))
    elements.append(Spacer(1, 1.5*cm)) 

    # สรุปยอดหนี้โดยรวม (ใช้ Table เพื่อจัดชิดขวา)
    data_summary = [
        [Paragraph(f"ยอดหนี้ทั้งหมด:", pdf_styles['Normal']), Paragraph(f"{total_loan:,.2f} บาท", pdf_styles['RightAlignAmount'])],
        [Paragraph(f"ยอดที่ชำระแล้วทั้งหมด:", pdf_styles['Normal']), Paragraph(f"{total_paid_all_years:,.2f} บาท", pdf_styles['RightAlignAmount'])],
        [Paragraph(f"ค่าปรับรวมทั้งหมด:", pdf_styles['Normal']), Paragraph(f"{total_fine_all_years:,.2f} บาท", pdf_styles['RightAlignAmount'])],
        [Paragraph(f"ยอดหนี้คงเหลือทั้งหมด (รวมค่าปรับ):", pdf_styles['BoldNormal']), Paragraph(f"<font face='{thai_font_name_bold}'>{remaining_overall_debt_with_fine:,.2f} บาท</font>", pdf_styles['RightAlignAmount'])]
    ]
    table_summary = Table(data_summary, colWidths=[10*cm, 6*cm]) 
    table_summary.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'), 
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5), 
    ]))
    elements.append(table_summary)
    
    elements.append(Spacer(1, 1.5*cm)) 
    elements.append(Paragraph("---", pdf_styles['Normal']))
    elements.append(Spacer(1, 1.0*cm)) 

    # ลายเซ็น: บรรทัดเดียวกัน ชิดซ้ายและขวา
    signature_table_data = [
        [Paragraph("___________________", pdf_styles['SignatureLeft']), Paragraph("___________________", pdf_styles['SignatureRight'])],
        [Paragraph("(&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ผู้ชำระเงิน&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;)", pdf_styles['SignatureLeft']), Paragraph("(&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ผู้รับเงิน&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;)", pdf_styles['SignatureRight'])]
    ]
    signature_table = Table(signature_table_data, colWidths=[8*cm, 8*cm]) # แบ่งครึ่งหน้ากระดาษ
    signature_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
    ]))
    elements.append(signature_table)

    elements.append(Spacer(1, 0.5*cm))

    doc.build(elements)
    buffer.seek(0)
    return buffer

# ========== STREAMLIT APP ==========
st.set_page_config(layout="wide", page_title="ระบบจัดการลูกหนี้หมู่บ้าน")
st.title("🏡 **ระบบจัดการลูกหนี้หมู่บ้าน**")

# Load data and initial setup
customers_df, payments_df, customer_amounts = load_data()

# Ensure debtor_names is correctly initialized even if customers_df is empty or 'NAME' is missing
if not customers_df.empty and 'NAME' in customers_df.columns:
    debtor_names = customers_df['NAME'].tolist()
else:
    debtor_names = []

if 'selected_debtor' not in st.session_state:
    st.session_state.selected_debtor = debtor_names[0] if debtor_names else None

menu = st.sidebar.radio("📋 **เมนูหลัก**", ["หน้าหลัก (บันทึก & สรุป)", "👤 จัดการลูกหนี้"])

# ========== PAGE: หน้าหลัก (บันทึก & สรุป) ==========
if menu == "หน้าหลัก (บันทึก & สรุป)":
    st.header("🏠 **หน้าหลัก: บันทึกการชำระ & สรุปสถานะหนี้**")

    selected_debtor_for_summary_and_input = st.selectbox(
        "เลือกชื่อลูกหนี้", 
        debtor_names, 
        key="global_debtor_select", 
        index=debtor_names.index(st.session_state.selected_debtor) if st.session_state.selected_debtor in debtor_names else (0 if debtor_names else None),
        on_change=lambda: st.session_state.update(selected_debtor=st.session_state.global_debtor_select)
    )
    if st.session_state.selected_debtor != selected_debtor_for_summary_and_input:
        st.session_state.selected_debtor = selected_debtor_for_summary_and_input
        st.rerun()

    st.markdown("---") 

    with st.expander("📝 **บันทึกการชำระเงินใหม่**", expanded=True):
        if not st.session_state.selected_debtor:
            st.warning("⚠️ กรุณาเพิ่มลูกหนี้ในเมนู 'จัดการลูกหนี้' ก่อนทำการบันทึกการชำระเงิน.")
        else:
            with st.form("payment_form", clear_on_submit=True):
                st.write(f"**กำลังบันทึกการชำระเงินสำหรับ**: **{st.session_state.selected_debtor}**")
                pay_date = st.date_input("วันที่จ่าย", datetime.today().date(), key="payment_date_input")
                amount = st.number_input("จำนวน (บาท)", min_value=0.0, step=100.0, key="payment_amount_input")
                note = st.text_input("หมายเหตุ", "", key="payment_note_input")
                submitted = st.form_submit_button("💾 บันทึกการชำระ") 

            if submitted: 
                new_payment_row = pd.DataFrame([{
                    "ชื่อลูกค้า": st.session_state.selected_debtor,
                    "วันที่จ่าย_str": pay_date.strftime("%Y-%m-%d"),
                    "วันที่จ่าย": pd.to_datetime(pay_date),
                    "จำนวนเงิน": amount,
                    "หมายเหตุ": note
                }])
                payments_df = pd.concat([payments_df, new_payment_row], ignore_index=True)
                save_payments_df(payments_df)

                st.success("✅ **บันทึกข้อมูลเรียบร้อยแล้ว!**")
                st.session_state.submitted_for_receipt = True
                st.session_state.last_payment_date = pay_date
                st.session_state.last_payment_amount = amount
                
                st.info("💡 ข้อมูลถูกบันทึกแล้ว โปรดรอสักครู่เพื่อสร้างใบเสร็จ PDF โดยอัตโนมัติ (หากคุณบันทึกในหน้าหลัก)")
                st.info("ถ้าไม่เห็นปุ่มดาวน์โหลดใบเสร็จ กรุณากดปุ่ม 'บันทึกการชำระ' อีกครั้งหลังจากที่หน้านี้โหลดเสร็จสมบูรณ์")

    if 'submitted_for_receipt' in st.session_state and st.session_state.submitted_for_receipt:
        if st.session_state.selected_debtor:
            customers_df_latest, payments_df_latest, _ = load_data()
            latest_selected_debtor_row = customers_df_latest[customers_df_latest['NAME'] == st.session_state.selected_debtor]
            
            if not latest_selected_debtor_row.empty:
                total_loan_for_receipt = latest_selected_debtor_row['AmountDue'].iloc[0]
                summary_data_receipt, total_paid_receipt, total_fine_receipt, remaining_overall_debt_with_fine_receipt = calculate_yearly_summary(
                    st.session_state.selected_debtor, total_loan_for_receipt, payments_df_latest
                )
                yearly_summary_df_receipt = pd.DataFrame(summary_data_receipt)

                pdf_buffer = create_receipt_pdf(
                    st.session_state.selected_debtor,
                    st.session_state.last_payment_date,
                    st.session_state.last_payment_amount,
                    yearly_summary_df_receipt,
                    total_loan_for_receipt,
                    total_paid_receipt,
                    remaining_overall_debt_with_fine_receipt, 
                    total_fine_receipt
                )
                st.download_button(
                    label="📄 พิมพ์ใบเสร็จ PDF",
                    data=pdf_buffer,
                    file_name=f"ใบเสร็จ_{st.session_state.selected_debtor}_{st.session_state.last_payment_date.strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    key="download_receipt_button" 
                )
                st.info("💡 คลิกปุ่ม 'พิมพ์ใบเสร็จ PDF' ด้านบนเพื่อดาวน์โหลดใบเสร็จ")
                st.session_state.submitted_for_receipt = False
                if 'last_payment_date' in st.session_state:
                    del st.session_state.last_payment_date
                if 'last_payment_amount' in st.session_state:
                    del st.session_state.last_payment_amount
            else:
                st.warning("⚠️ ไม่พบข้อมูลลูกหนี้สำหรับสร้างใบเสร็จ โปรดตรวจสอบชื่อลูกหนี้")
        else:
            st.warning("⚠️ กรุณาเลือกชื่อลูกหนี้ที่ต้องการพิมพ์ใบเสร็จ.")

    st.markdown("---") 

    st.subheader("📊 **สรุปสถานะหนี้ลูกหนี้**")
    summary_debtor_name = st.session_state.selected_debtor 

    if summary_debtor_name:
        selected_debtor_row = customers_df[customers_df['NAME'] == summary_debtor_name]
        if not selected_debtor_row.empty:
            total_loan_for_summary = selected_debtor_row['AmountDue'].iloc[0]
            
            summary_data, total_paid, total_fine, remaining_overall_debt_with_fine = calculate_yearly_summary( 
                summary_debtor_name, total_loan_for_summary, payments_df
            )

            st.markdown(
                f"""
                <div style="background-color:#d4edda; padding:15px; border-radius:10px; border:2px solid #28a745;">
                    <h4 style="color:#155724; text-align:center;">**ข้อมูลหนี้ของ: {summary_debtor_name}**</h4>
                    <div style="display:flex; justify-content:space-around; margin-top:10px;">
                        <div style="text-align:center; flex:1;">
                            <p style="font-size:1.1em; color:#155724;">💰 **รวมยอดหนี้ทั้งหมด**</p>
                            <h3 style="color:#155724;">{total_loan_for_summary:,.2f} บาท</h3>
                        </div>
                        <div style="text-align:center; flex:1;">
                            <p style="font-size:1.1em; color:#856404;">✅ **ยอดที่จ่ายแล้ว (รวมทุกปี)**</p>
                            <h3 style="color:#856404;">{total_paid:,.2f} บาท</h3>
                        </div>
                        <div style="text-align:center; flex:1;">
                            <p style="font-size:1.1em; color:#721c24;">🔻 **ค่าปรับรวมทั้งหมด**</p>
                            <h3 style="color:#721c24;">{total_fine:,.2f} บาท</h3>
                        </div>
                        <div style="text-align:center; flex:1;">
                            <p style="font-size:1.1em; color:#004085;">📌 **ยอดหนี้คงเหลือ (รวมค่าปรับ)**</p>
                            <h3 style="color:#004085;">{remaining_overall_debt_with_fine:,.2f} บาท</h3>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True
            )

            st.markdown("---")
            st.subheader("🗓️ **รายละเอียดการชำระรายปี**")
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

            st.markdown("---")
            st.subheader("📜 **ประวัติการชำระเงินทั้งหมด**")
            payment_history_for_display = payments_df[payments_df['ชื่อลูกค้า'] == summary_debtor_name].sort_values(by='วันที่จ่าย', ascending=False)
            
            if not payment_history_for_display.empty:
                if 'show_edit_form' not in st.session_state:
                    st.session_state.show_edit_form = False
                if 'edit_payment_data' not in st.session_state:
                    st.session_state.edit_payment_data = None

                for i, row in payment_history_for_display.iterrows():
                    original_df_row_index = row['temp_index']
                    row_index_in_gsheet = payments_df[payments_df['temp_index'] == original_df_row_index].index[0] + 2

                    edit_button_key = f"edit_payment_{original_df_row_index}"
                    receipt_button_key = f"print_receipt_{original_df_row_index}"

                    with st.expander(f"**รายการวันที่ {row['วันที่จ่าย'].strftime('%d/%m/%Y')} จำนวน {row['จำนวนเงิน']:,.0f} บาท**", expanded=False):
                        st.write(f"**ผู้จ่าย**: {row['ชื่อลูกค้า']}")
                        st.write(f"**วันที่จ่าย**: {row['วันที่จ่าย'].strftime('%d/%m/%Y')}")
                        st.write(f"**จำนวน**: {row['จำนวนเงิน']:,.0f} บาท")
                        st.write(f"**หมายเหตุ**: {row['หมายเหตุ'] if row['หมายเหตุ'] else '-'}")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("✏️ **แก้ไขรายการนี้**", key=edit_button_key):
                                st.session_state.edit_payment_data = {
                                    "sheet_row_index": row_index_in_gsheet, 
                                    "debtor_name": row['ชื่อลูกค้า'], 
                                    "payment_date": row['วันที่จ่าย'].date(), 
                                    "amount": row['จำนวนเงิน'], 
                                    "note": row['หมายเหตุ'] 
                                }
                                st.session_state.show_edit_form = True
                                st.rerun() 
                        with col2:
                            customers_df_receipt, payments_df_receipt, _ = load_data() 
                            selected_debtor_row_receipt = customers_df_receipt[customers_df_receipt['NAME'] == summary_debtor_name]
                            total_loan_receipt = selected_debtor_row_receipt['AmountDue'].iloc[0]
                            
                            summary_data_receipt, total_paid_all_receipt, total_fine_all_receipt, remaining_overall_debt_with_fine_receipt = calculate_yearly_summary(
                                summary_debtor_name, total_loan_receipt, payments_df_receipt
                            )
                            yearly_summary_df_receipt = pd.DataFrame(summary_data_receipt)

                            receipt_pdf_buffer = create_receipt_pdf(
                                debtor_name=row['ชื่อลูกค้า'], 
                                payment_date=row['วันที่จ่าย'].date(),
                                amount_paid=row['จำนวนเงิน'], 
                                yearly_summary_df=yearly_summary_df_receipt,
                                total_loan=total_loan_receipt,
                                total_paid_all_years=total_paid_all_receipt,
                                remaining_overall_debt_with_fine=remaining_overall_debt_with_fine_receipt, 
                                total_fine_all_years=total_fine_all_receipt
                            )

                            st.download_button(
                                label="📄 พิมพ์ใบเสร็จสำหรับรายการนี้",
                                data=receipt_pdf_buffer,
                                file_name=f"ใบเสร็จ_{row['ชื่อลูกค้า']}_{row['วันที่จ่าย'].strftime('%Y%m%d')}_{row['จำนวนเงิน']}.pdf", 
                                mime="application/pdf",
                                key=receipt_button_key
                            )

            else:
                st.info("ℹ️ ลูกหนี้ยังไม่มีประวัติการชำระเงิน")
        else:
            st.info("ℹ️ กรุณาเลือกชื่อลูกหนี้ที่ถูกต้องในช่องด้านบน เพื่อดูสรุปสถานะหนี้")
    else:
        st.info("ℹ️ กรุณาเลือกชื่อลูกหนี้เพื่อเริ่มต้น")

    if 'show_edit_form' in st.session_state and st.session_state.show_edit_form:
        edit_data = st.session_state.edit_payment_data
        
        with st.form("edit_payment_dialog", clear_on_submit=False):
            st.subheader(f"แก้ไขรายการชำระเงินสำหรับ: {edit_data['debtor_name']}")
            edited_date = st.date_input("วันที่จ่าย", edit_data['payment_date'], key="edit_dialog_pay_date_input")
            edited_amount = st.number_input("จำนวน (บาท)", value=float(edit_data['amount']), min_value=0.0, step=100.0, key="edit_dialog_pay_amount_input")
            edited_note = st.text_input("หมายเหตุ", value=edit_data['note'], key="edit_dialog_pay_note_input")

            col_edit1, col_edit2 = st.columns(2)
            with col_edit1:
                update_submitted = st.form_submit_button("✅ อัปเดตการชำระเงิน")
            with col_edit2:
                cancel_edit = st.form_submit_button("❌ ยกเลิก")

            if update_submitted:
                updated_payment_list_for_gsheet = [
                    edit_data['debtor_name'], 
                    edited_date.strftime("%Y-%m-%d"), 
                    edited_amount, 
                    edited_note 
                ]
                try:
                    client = get_gspread_client()
                    ws = client.open_by_url(GSHEET_URL).worksheet("pay")
                    ws.update(f'A{edit_data["sheet_row_index"]}:D{edit_data["sheet_row_index"]}', [updated_payment_list_for_gsheet])
                    
                    st.success("✅ **อัปเดตข้อมูลการชำระเงินเรียบร้อยแล้ว!**")
                    st.session_state.show_edit_form = False 
                    clear_cache_and_rerun() 
                except gspread.exceptions.APIError as e:
                    st.error(f"❌ **เกิดข้อผิดพลาดในการอัปเดตข้อมูล**: {e}")
            
            if cancel_edit:
                st.session_state.show_edit_form = False
                st.rerun() 

# ========== PAGE: จัดการลูกหนี้ ==========
elif menu == "👤 จัดการลูกหนี้":
    st.header("⚙️ **จัดการข้อมูลลูกหนี้**")

    with st.expander("➕ **เพิ่มลูกหนี้ใหม่**", expanded=True):
        with st.form("add_debtor", clear_on_submit=True):
            new_name = st.text_input("ชื่อลูกหนี้ใหม่", key="add_debtor_name_input")
            new_total = st.number_input("รวมเงินกู้ทั้งหมด (บาท)", min_value=0.0, step=1000.0, key="add_debtor_total_input")
            add_btn = st.form_submit_button("✅ เพิ่มลูกหนี้")

        if add_btn:
            if new_name in debtor_names:
                st.warning("⚠️ ชื่อนี้มีอยู่แล้ว กรุณาใช้ชื่ออื่น")
            else:
                new_id = customers_df['ลำดับที่'].max() + 1 if not customers_df.empty else 1
                new_customer_row = pd.DataFrame([{
                    'ลำดับที่': new_id, 
                    'NAME': new_name, 
                    'AmountDue': new_total
                }])
                updated_df = pd.concat([customers_df, new_customer_row], ignore_index=True)
                save_customers_df(updated_df) 
                st.success(f"🎉 **เพิ่มลูกหนี้ {new_name} เรียบร้อยแล้ว!**")
    
    st.markdown("---") 

    with st.expander("✏️ **แก้ไขยอดเงินกู้ทั้งหมดของลูกหนี้**", expanded=True):
        if not debtor_names:
            st.info("ℹ️ ยังไม่มีลูกหนี้ให้แก้ไข")
        else:
            debtor_to_edit_loan = st.selectbox("เลือกชื่อลูกหนี้ที่ต้องการแก้ไขยอดเงินกู้", debtor_names, key="edit_loan_debtor_select")
            if debtor_to_edit_loan:
                current_loan = customers_df[customers_df['NAME'] == debtor_to_edit_loan]['AmountDue'].iloc[0] 
                new_loan_amount = st.number_input(f"ยอดเงินกู้ใหม่สำหรับ {debtor_to_edit_loan} (ปัจจุบัน: {current_loan:,.2f} บาท)", 
                                                value=float(current_loan), min_value=0.0, step=1000.0, key="new_loan_amount_input")
                if st.button("💾 บันทึกการแก้ไขยอดเงินกู้", key="save_edited_loan_button"):
                    customers_df.loc[customers_df['NAME'] == debtor_to_edit_loan, 'AmountDue'] = new_loan_amount 
                    save_customers_df(customers_df) 
                    st.success(f"✅ **แก้ไขยอดเงินกู้ของ {debtor_to_edit_loan} เป็น {new_loan_amount:,.2f} บาท เรียบร้อยแล้ว!**")
    
    st.markdown("---") 

    st.subheader("📋 **รายชื่อลูกหนี้ทั้งหมด**")
    # This is the corrected line that was causing the error
    display_debtors_df = customers_df[['ลำดับที่', 'NAME', 'AmountDue']].copy()
    
    # Ensure 'AmountDue' is formatted as a string for display if needed
    # It's already numeric, so .copy() ensures we don't modify the original df
    if 'AmountDue' in display_debtors_df.columns:
        display_debtors_df["AmountDue"] = display_debtors_df["AmountDue"].apply(lambda x: f"{x:,.2f}")

    # Rename columns for display to match the Thai names in the Google Sheet configuration
    display_debtors_df.rename(columns={
        "ลำดับที่": "NO", 
        "NAME": "NAME", 
        "AmountDue": "LOAN"
    }, inplace=True)
    
    st.dataframe(display_debtors_df, use_container_width=True, hide_index=True)
    
    st.markdown("---") 

    with st.expander("🗑️ **ลบลูกหนี้**", expanded=False): 
        if not debtor_names:
            st.info("ℹ️ ไม่มีลูกหนี้ให้ลบ")
        else:
            debtor_to_delete = st.selectbox("เลือกชื่อลูกหนี้ที่ต้องการลบ", debtor_names, key="delete_debtor_select")
            
            # เพิ่ม checkbox ยืนยันการลบ
            st.warning(f"⚠️ การดำเนินการนี้จะลบข้อมูลของ **{debtor_to_delete}** และประวัติการชำระเงินทั้งหมดออกไปอย่างถาวร")
            confirm_delete = st.checkbox(f"ฉันเข้าใจและยืนยันที่จะลบข้อมูลลูกหนี้ '{debtor_to_delete}' รายนี้อย่างถาวร", key="confirm_delete_checkbox")
            
            if st.button("❗ ลบลูกหนี้", key="delete_debtor_button", disabled=not confirm_delete):
                if confirm_delete:
                    # ลบจาก customers_df
                    updated_customers_df = customers_df[customers_df['NAME'] != debtor_to_delete].copy() 
                    save_customers_df(updated_customers_df) # ฟังก์ชัน save_customers_df จะเรียก st.rerun() อยู่แล้ว

                    # ลบประวัติการชำระเงินทั้งหมดของลูกหนี้รายนี้
                    try:
                        client = get_gspread_client()
                        pay_sheet = client.open_by_url(GSHEET_URL).worksheet("pay")
                        
                        # Get all values to find rows to delete
                        all_pay_values = pay_sheet.get_all_values()
                        if not all_pay_values:
                            st.success(f"🗑️ **ลบลูกหนี้ {debtor_to_delete} และประวัติการชำระเงิน (ไม่มีประวัติ) เรียบร้อยแล้ว!**")
                        else:
                            pay_headers = [_normalize_gsheet_col_name(h) for h in all_pay_values[0]]
                            pay_data_to_keep = [all_pay_values[0]] # Keep headers
                            
                            # Find the index of the 'ผู้จ่าย' column safely
                            payer_col_index = -1
                            for i, h in enumerate(pay_headers):
                                if h == "ผู้จ่าย":
                                    payer_col_index = i
                                    break
                            
                            if payer_col_index != -1: # Ensure column is found
                                for row_idx, row_data in enumerate(all_pay_values[1:]): # Start from 1 to skip headers
                                    if len(row_data) > payer_col_index and _normalize_gsheet_col_name(row_data[payer_col_index]) != debtor_to_delete:
                                        pay_data_to_keep.append(row_data)
                            else:
                                st.warning("ไม่พบคอลัมน์ 'ผู้จ่าย' ในชีต 'pay'. ไม่สามารถลบประวัติการชำระเงินของลูกหนี้.")
                                pay_data_to_keep = all_pay_values # If column not found, keep all data to prevent data loss


                            # Clear sheet and write back only the data we want to keep
                            pay_sheet.clear()
                            if pay_data_to_keep: # Ensure there is data to write back
                                pay_sheet.update(pay_data_to_keep)
                            
                            st.success(f"🗑️ **ลบลูกหนี้ {debtor_to_delete} และประวัติการชำระเงินทั้งหมดเรียบร้อยแล้ว!**")
                            clear_cache_and_rerun() # Rerun one more time to ensure all caches are clear
                            
                    except Exception as e:
                        st.error(f"❌ **เกิดข้อผิดพลาดในการลบประวัติการชำระเงิน**: {e}")
                        st.warning("⚠️ ลูกหนี้ถูกลบแล้ว แต่อาจมีประวัติการชำระเงินค้างอยู่ใน Google Sheet 'pay' โปรดตรวจสอบด้วยตนเอง")
                else:
                    st.warning("โปรดติ๊กช่องยืนยันการลบก่อน")