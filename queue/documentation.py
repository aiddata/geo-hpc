# accepts request object and creates pdf documentation

import os
import time

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER


class doc():


    def __init__(self):
        self.dir_base = os.path.dirname(os.path.abspath(__file__))
		
		self.doc = SimpleDocTemplate("/home/userx/Desktop/simple_table.pdf", pagesize=letter)

		# container for the 'Flowable' objects
		self.Story = []

		self.styles = getSampleStyleSheet()
		self.styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
		self.styles.add(ParagraphStyle(name='Center', alignment=TA_CENTER))



    def build_doc(self, rid):

        print "build_doc"

        try:
        	# build doc call all functions
        	# 

        	return True
        except:
	        return False




	def add_header(self):
		# aiddata logo
		logo = dir_base + "/templates/logo.png"

		im = Image(logo, 2.188*inch, 0.5*inch)
		im.hAlign = 'LEFT'
		Story.append(im)

		Story.append(Spacer(1, 0.25*inch))

		# title
		ptext = '<font size=20>Data Extraction Tool Request Documentation</font>'
		Story.append(Paragraph(ptext, styles["Center"]))
		Story.append(Spacer(1, 0.5*inch))


	def add_info(self):
		# report generation info
		ptext = '<font size=12>Report Info:</font>'
		Story.append(Paragraph(ptext, styles['BodyText']))
		Story.append(Spacer(1, 0.1*inch))

		data = [['Request', 'id'],
		       ['Email', 'email'],
		       ['Generated on', time.strftime('%Y-%m-%d %H:%M:%S (%Z)', time.localtime())]]

		t = Table(data)

		t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
								('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

		Story.append(t)

		Story.append(Spacer(1,0.3*inch))


	def add_general(self):

		# intro paragraphs
		with open(dir_base + '/templates/general.txt') as general:
			for line in general:
				p = Paragraph(line, styles['BodyText'])
				Story.append(p)

		Story.append(Spacer(1,0.3*inch))


	def add_readme(self):

		# general readme

		with open(dir_base + '/templates/readme.txt') as readme:
			for line in readme:
				p = Paragraph(line, styles['BodyText'])
				Story.append(p)

		Story.append(Spacer(1,0.3*inch))



	def add_overview(self):


		# request overview

		ptext = '<b><font size=12>Request Overview</font><b>'
		Story.append(Paragraph(ptext, styles['Normal']))
		Story.append(Spacer(1, 0.15*inch))

		# boundary
		ptext = '<i>Boundary</i>'
		Story.append(Paragraph(ptext, styles['Normal']))
		Story.append(Spacer(1, 0.05*inch))

		data = [['Title (Name: Group)','blank'],
		       ['Description','blank'],
		       ['Source Link','blank']]

		t = Table(data)
		t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
								('BOX', (0,0), (-1,-1), 0.25, colors.black)]))
		Story.append(t)
		Story.append(Spacer(1, 0.1*inch))


		# datasets
		for i in [0,1]:
			ptext = '<i>Dataset '+str(i)+'</i>'
			Story.append(Paragraph(ptext, styles['Normal']))
			Story.append(Spacer(1, 0.05*inch))

			data = [['Title (Name)','blank'],
			       ['Type','blank'],
			       ['Items Requested','blank'],
			       ['Extract Types Selected','blank'],
			       ['Files','blank']]

			t = Table(data)
			t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
									('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

			Story.append(t)
			Story.append(Spacer(1, 0.1*inch))


		Story.append(Spacer(1, 0.3*inch))


	def add_meta(self):

		ptext = '<b><font size=12>Meta Information</font></b>'
		Story.append(Paragraph(ptext, styles['Normal']))
		Story.append(Spacer(1, 0.15*inch))

		# full boundary meta

		ptext = '<i>Boundary </i>'
		Story.append(Paragraph(ptext, styles['Normal']))
		Story.append(Spacer(1, 0.05*inch))

		data = [['Title (Name: Group)','blank'],
		       ['Description','blank'],
		       ['Source Link','blank']]


		t = Table(data)
		t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
								('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

		Story.append(t)
		Story.append(Spacer(1, 0.1*inch))


		# full dataset meta

		for i in [0,1]:
			ptext = '<i>Dataset '+str(i)+'</i>'
			Story.append(Paragraph(ptext, styles['Normal']))
			Story.append(Spacer(1, 0.05*inch))

			data = [['Title (Name)','blank'],
			       ['Type','blank'],
			       ['Items Requested','blank'],
			       ['Extract Types Selected','blank'],
			       ['Files','blank']]



			t = Table(data)
			t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
									('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

			Story.append(t)
			Story.append(Spacer(1, 0.1*inch))


		Story.append(Spacer(1, 0.3*inch))




	def add_timeline(self):

		# full request timeline / other processing info 

		ptext = '<b><font size=12>request timeline info</font></b>'
		Story.append(Paragraph(ptext, styles['Normal']))
		data = [['submit','blank'],
		       ['prep','blank'],
		       ['process','blank'],
		       ['complete','blank']]


		t = Table(data)

		t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
								('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

		Story.append(t)

		Story.append(Spacer(1, 0.3*inch))


	def add_license(self):

		# license stuff

		with open(dir_base + '/templates/license.txt') as license:
			for line in license:
				p = Paragraph(line, styles['BodyText'])
				Story.append(p)

		Story.append(Spacer(1,0.3*inch))



	def output_doc(self):

		# write the document to disk
		self.doc.build(self.Story)


