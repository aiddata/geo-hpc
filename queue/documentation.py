# accepts request object and creates pdf documentation

import os
import time

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER

import pymongo

class doc():


    def __init__(self):
        self.dir_base = os.path.dirname(os.path.abspath(__file__))
        
        self.doc = 0

        self.request = 0

        # container for the 'Flowable' objects
        self.Story = []

        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
        self.styles.add(ParagraphStyle(name='Center', alignment=TA_CENTER))

        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.c_asdf = self.client.asdf.data


    def time_str(self, timestamp=None):
        if timestamp != None:
            try:
                timestamp = int(timestamp)
            except:
                return "---"

        return time.strftime('%Y-%m-%d %H:%M:%S (%Z)', time.localtime(timestamp))


    def build_doc(self, rid):

        print 'build_doc: ' + rid

        # try:
        # self.doc = SimpleDocTemplate('/sciclone/aiddata10/REU/det/results/documentation.pdf', pagesize=letter)

        self.doc = SimpleDocTemplate('/sciclone/aiddata10/REU/det/results/'+rid+'/documentation.pdf', pagesize=letter)
        
        # build doc call all functions
        self.add_header()
        self.add_info()
        self.add_general()
        self.add_readme()
        self.add_overview()
        self.add_meta()
        self.add_timeline()
        self.add_license()
        self.output_doc()

        return True
        # except:
        #     return False



    # documentation header 
    def add_header(self):
        # aiddata logo
        logo = self.dir_base + '/templates/logo.png'

        im = Image(logo, 2.188*inch, 0.5*inch)
        im.hAlign = 'LEFT'
        self.Story.append(im)

        self.Story.append(Spacer(1, 0.25*inch))

        # title
        ptext = '<font size=20>Data Extraction Tool Request Documentation</font>'
        self.Story.append(Paragraph(ptext, self.styles['Center']))
        self.Story.append(Spacer(1, 0.5*inch))


    # report generation info
    def add_info(self):
        ptext = '<font size=12>Report Info:</font>'
        self.Story.append(Paragraph(ptext, self.styles['BodyText']))
        self.Story.append(Spacer(1, 0.1*inch))

        data = [['Request', str(self.request['_id'])],
               ['Email', self.request['email']],
               ['Generated on', self.time_str()]]

        t = Table(data)

        t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                                ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

        self.Story.append(t)

        self.Story.append(Spacer(1,0.3*inch))


    # intro paragraphs
    def add_general(self):

        with open(self.dir_base + '/templates/general.txt') as general:
            for line in general:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)

        self.Story.append(Spacer(1,0.3*inch))


    # general readme
    def add_readme(self):

        with open(self.dir_base + '/templates/readme.txt') as readme:
            for line in readme:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)

        self.Story.append(Spacer(1,0.3*inch))


    # request overview
    def add_overview(self):

        ptext = '<b><font size=12>Request Overview</font></b>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.15*inch))

        # boundary
        ptext = '<i>Boundary - '+self.request['boundary']['name']+'</i>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.05*inch))

        data = [['Title (Name: Group)',  self.request['boundary']['title'] +' ('+ self.request['boundary']['name'] +' : '+  self.request['boundary']['group'] +')'],
                ['Description',  self.request['boundary']['short']],
                ['Source Link',  self.request['boundary']['source_link']]]

        t = Table(data)
        t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                              ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))
        self.Story.append(t)
        self.Story.append(Spacer(1, 0.1*inch))


        # datasets
        for dset in self.request['data'].values():

            ptext = '<i>Dataset - '+dset['name']+'</i>'
            self.Story.append(Paragraph(ptext, self.styles['Normal']))
            self.Story.append(Spacer(1, 0.05*inch))

            data = [['Title (Name)',dset['title'] +' ('+ dset['name'] +')'],
                    ['Type', dset['type']],
                    ['Items Requested', self.request['counts'][dset['name']]],
                    ['Temporal Type', dset['temporal_type']],
                    ['Files', ', '.join([f['name'] for f in dset['files']])]]

            if dset['type'] == 'raster':
                data.append(['Extract Types Selected', ', '.join(dset['options']['extract_types'])])

            t = Table(data)
            t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                                    ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

            self.Story.append(t)
            self.Story.append(Spacer(1, 0.1*inch))


        self.Story.append(Spacer(1, 0.3*inch))

    def build_meta(self, name, item_type):

        # get meta from asdf
        meta = self.c_asdf.find({'name': name})

        # build generic meta
        data = [

            ['Title', 'blank'],
            ['Name', 'blank'],
            ['Version', 'blank'],
            ['Mini Name', 'blank'],
            ['Short', 'blank'],
            ['Variable Description', 'blank'],
            ['Source Link', 'blank'],

            ['Type', 'blank'],
            ['File Format', 'blank'],
            ['File Extension', 'blank'],
            ['Scale', 'blank'],

            ['Temporal', '***'],
            ['Bounding Box', '***'],

            ['Sources', '***'],
            ['Licenses', '***'],

            ['Maintainers', '***'],
            ['Publishers', '***'],

            ['Date Added', 'blank'],
            ['Date Updated', 'blank'],

        ]


        if item_type == 'boundary':
            data.append(['Group', 'blank'])
            data.append(['Group Class', 'blank'])

        elif item_type == 'raster':
            data.append(['Resolution', 'blank'])
            data.append(['Extract Types', ', '.join([])])
            data.append(['Factor', 'blank'])


        return data


    def add_meta(self):

        ptext = '<b><font size=12>Meta Information</font></b>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.15*inch))

        # full boundary meta
        ptext = '<i>Boundary </i>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.05*inch))


        # build boundary meta table array
        data = self.build_meta(self.request['boundary']['name'], 'boundary')


        t = Table(data)
        t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                                ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

        self.Story.append(t)
        self.Story.append(Spacer(1, 0.1*inch))


        # full dataset meta
        for dset in self.request['data'].values():
            

            ptext = '<i>Dataset - '+dset['name']+'</i>'
            self.Story.append(Paragraph(ptext, self.styles['Normal']))
            self.Story.append(Spacer(1, 0.05*inch))



            # build dataset meta table array
            data = self.build_meta(dset['name'], dset['type'])


            t = Table(data)
            t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                                  ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

            self.Story.append(t)
            self.Story.append(Spacer(1, 0.1*inch))


        self.Story.append(Spacer(1, 0.3*inch))




    # full request timeline / other processing info 
    def add_timeline(self):

        ptext = '<b><font size=12>request timeline info</font></b>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        data = [
                ['submit', self.time_str(self.request['submit_time'])],
                ['prep', self.time_str(self.request['prep_time'])],
                ['process', self.time_str(self.request['process_time'])],
                ['complete', self.time_str(self.request['complete_time'])]
            ]


        t = Table(data)

        t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                              ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

        self.Story.append(t)

        self.Story.append(Spacer(1, 0.3*inch))


    # license stuff
    def add_license(self):

        with open(self.dir_base + '/templates/license.txt') as license:
            for line in license:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)

        self.Story.append(Spacer(1,0.3*inch))



    # write the document to disk
    def output_doc(self):
        self.doc.build(self.Story)

