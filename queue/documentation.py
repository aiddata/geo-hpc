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
                ['Description',  self.request['boundary']['description']],
                ['Source Link',  self.request['boundary']['source_link']]]

        t = Table(data)
        t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                              ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))
        self.Story.append(t)
        self.Story.append(Spacer(1, 0.1*inch))

        # datasets

        msr_field_id = 1
        for i in sorted(self.request['d1_data'].keys()):
        # for dset in self.request['d1_data'].values():
            dset = self.request['d1_data'][i]

            ptext = '<i>Dataset - '+dset['dataset']+'</i>'
            self.Story.append(Paragraph(ptext, self.styles['Normal']))
            self.Story.append(Spacer(1, 0.05*inch))

            data = [['Dataset ',dset['dataset']],
                    ['Type', dset['type']],
                    ['Donors', ', '.join(dset['donors'])],
                    ['Sectors', ', '.join(dset['sectors'])],
                    ['Years', ', '.join(dset['years'])],
                    ['Extract Field Name', 'ad_msr' + '{0:03d}'.format(msr_field_id)+'s'],
                    ['Reliability Field Name', 'ad_msr' + '{0:03d}'.format(msr_field_id)+'r']
                    ]

            t = Table(data)
            t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                                    ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

            self.Story.append(t)
            self.Story.append(Spacer(1, 0.1*inch))

            msr_field_id += 1


        for dset in self.request['d2_data'].values():

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
        meta = self.c_asdf.find({'name': name})[0]

        # build generic meta
        data = [

            ['Title', meta['title']],
            ['Name', meta['name']],
            ['Version', meta['version']],
            ['Description', meta['description']],
            ['Source Link', meta['source_link']],

            ['Type', meta['type']],
            ['File Format', meta['file_format']],
            ['File Extension', meta['file_extension']],
            ['Scale', meta['scale']],
            ['Temporal']
        ]


        data.append(['Temporal Type', meta['temporal']['name']])

        if meta['temporal']['format'] != 'None':
            data.append(['Temporal Start', meta['temporal']['start']])
            data.append(['Temporal End', meta['temporal']['end']])
            data.append(['Temporal Format', meta['temporal']['format']])

            
        data.append(['Bounding Box', Paragraph(str(meta['spatial']['coordinates']), self.styles['Normal'])])


        for i in range(len(meta['sources'])):
            data.append(['Source #'+str(i+1), Paragraph('<i>name:</i> '+meta['sources'][i]['name']+'<br /><i>web:</i> '+meta['sources'][i]['web'], self.styles['Normal'])])
        
        for i in range(len(meta['licenses'])):
            data.append(['License #'+str(i+1), Paragraph('<i>name:</i> '+meta['licenses'][i]['name']+'<br /><i>version:</i> '+meta['licenses'][i]['version']+'<br /><i>url:</i> '+meta['licenses'][i]['url'], self.styles['Normal'])])
            

        for i in range(len(meta['maintainers'])):
            data.append(['Maintainer #'+str(i+1), Paragraph('<i>name:</i> '+meta['maintainers'][i]['name']+'<br /><i>web:</i> '+meta['maintainers'][i]['web']+'<br /><i>email:</i> '+meta['maintainers'][i]['email'], self.styles['Normal'])])
            
        for i in range(len(meta['publishers'])):
            data.append(['Publisher #'+str(i+1), Paragraph('<i>name:</i> '+meta['publishers'][i]['name']+'<br /><i>web:</i> '+meta['publishers'][i]['web']+'<br /><i>email:</i> '+meta['publishers'][i]['email'], self.styles['Normal'])])

        data.append(['Date Added', meta['date_added']])
        data.append(['Date Updated', meta['date_updated']])



        if item_type == 'boundary':
            data.append(['Group', meta['options']['group']])
            data.append(['Group Class', meta['options']['group_class']])

        elif item_type == 'raster':
            data.append(['Mini Name', meta['options']['mini_name']])
            data.append(['Variable Description', meta['options']['variable_description']])
            data.append(['Resolution', meta['options']['resolution']])
            data.append(['Extract Types', ', '.join(meta['options']['extract_types'])])
            data.append(['Factor', meta['options']['factor']])
        
        elif item_type == 'release':
            download_link = 'https://github.com/AidData-WM/public_datasets/tree/master/geocoded' + meta['data_set_preamble'] +'_'+ meta['data_type'] +'_v'+ str(meta['version']) + '.zip'
            data.append(['Download Link', download_link])


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

        d1_meta_log = []
        for dset in self.request['d1_data'].values():
            
            if dset['dataset'] not in d1_meta_log:
                d1_meta_log.append(dset['dataset'])

                ptext = '<i>Dataset - '+dset['dataset']+'</i>'
                self.Story.append(Paragraph(ptext, self.styles['Normal']))
                self.Story.append(Spacer(1, 0.05*inch))

                # build dataset meta table array
                data = self.build_meta(dset['dataset'], dset['type'])

                t = Table(data)
                t.setStyle(TableStyle([('INNERGRID', (0,0), (-1,-1), 0.25, colors.black), 
                                      ('BOX', (0,0), (-1,-1), 0.25, colors.black)]))

                self.Story.append(t)
                self.Story.append(Spacer(1, 0.1*inch))


        for dset in self.request['d2_data'].values():
            

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

