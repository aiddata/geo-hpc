# accepts request object and creates pdf documentation

import os
import time
import pymongo

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER


# =============================================================================

styles = getSampleStyleSheet()

def pg(text, pg_type):
    """return paragraph of specified type for given text
    """
    if pg_type == 1:
        return Paragraph(text, styles['Normal'])
    elif pg_type == 2:
        return Paragraph(text, styles['BodyText'])
    else:
        raise Exception("invalid paragraph type")


class DocBuilder():

    def __init__(self, config, request, output, download_server):

        self.config = config
        self.client = config.client
        self.c_asdf = self.client.asdf.data

        self.request_id = str(request['_id'])
        self.request = request
        self.output = output

        self.assets_dir = os.path.join(config.source_dir, 'geo-hpc/assets')

        self.download_server = download_server

        self.doc = 0

        # container for the 'Flowable' objects
        self.Story = []

        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
        self.styles.add(ParagraphStyle(name='Center', alignment=TA_CENTER))



    def time_str(self, timestamp=None):
        if timestamp != None:
            try:
                timestamp = int(timestamp)
                if timestamp == 0:
                    return "---"
            except:
                return "---"

        return time.strftime('%Y-%m-%d %H:%M:%S (%Z)', time.localtime(timestamp))


    def build_doc(self):

        rid = self.request_id
        print 'build_doc: ' + rid

        # try:

        self.doc = SimpleDocTemplate(self.output, pagesize=letter)

        # build doc call all functions

        self.add_header()
        self.Story.append(Spacer(1, 0.5*inch))
        self.add_info()
        self.Story.append(Spacer(1, 0.3*inch))
        self.add_timeline()
        self.Story.append(Spacer(1, 0.3*inch))
        self.add_cite_and_contents()
        self.Story.append(PageBreak())

        self.add_meta()
        self.Story.append(PageBreak())

        self.add_notes()
        self.Story.append(PageBreak())

        self.add_additional()

        self.output_doc()

        return True
        # except:
        #     return False


    # documentation header
    def add_header(self):
        # aiddata logo
        logo = self.assets_dir + '/templates/aid_data.png'

        im = Image(logo, 2.188*inch, 0.5*inch)
        im.hAlign = 'LEFT'
        self.Story.append(im)

        self.Story.append(Spacer(1, 0.25*inch))

        # title
        ptext = '<font size=20>AidData GeoQuery Request Documentation</font>'
        self.Story.append(Paragraph(ptext, self.styles['Center']))


    # report generation info
    def add_info(self):
        ptext = '<b><font size=14>Report Info</font></b>'
        self.Story.append(Paragraph(ptext, self.styles['BodyText']))
        self.Story.append(Spacer(1, 0.1*inch))

        data = [
            ['Request Name', self.request['custom_name']],
            ['Request Id', str(self.request['_id'])],
            ['Email', self.request['email']],
            ['Generated on', self.time_str()],
            ['Download Link', '<a href="http://{0}/query/#!/status/{1}">{0}/query/#!/status/{1}</a>'.format(
                self.download_server, self.request['_id'])]
        ]

        data = [[i[0], pg(i[1], 1)] for i in data]
        t = Table(data)

        t.setStyle(TableStyle([
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black)
        ]))

        self.Story.append(t)



    # full request timeline / other processing info
    def add_timeline(self):

        ptext = '<b><font size=14>Processing Timeline</font></b>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.1*inch))

        data = [
            [self.request['stage'][0]['name'], self.time_str(self.request['stage'][0]['time'])],
            [self.request['stage'][1]['name'], self.time_str(self.request['stage'][1]['time'])],
            [self.request['stage'][2]['name'], self.time_str(self.request['stage'][2]['time'])],
            [self.request['stage'][3]['name'], self.time_str(int(time.time()))]
            # ['complete', self.time_str(self.request['stage'][3]['time'])]
        ]

        data = [[i[0], pg(i[1], 1)] for i in data]
        t = Table(data)

        t.setStyle(TableStyle([
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black)
        ]))

        self.Story.append(t)

    def add_cite_and_contents(self):

        with open(self.assets_dir + '/templates/general.txt') as general:
            for line in general:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)


    # intro paragraphs
    def add_notes(self):

        with open(self.assets_dir + '/templates/field_names.txt') as field_names:
            for line in field_names:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)

        self.Story.append(PageBreak())

        with open(self.assets_dir + '/templates/notes.txt') as field_names:
            for line in field_names:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)

        self.Story.append(PageBreak())

        with open(self.assets_dir + '/templates/aid_data.txt') as field_names:
            for line in field_names:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)



    def build_meta(self, name, item_type, dset):

        # get metadata for dataset from asdf->data collection
        meta = self.c_asdf.find_one({'name': name})

        if meta is None:
            msg = ('Could not lookup dataset ({0}, {1}) for '
                   'build_meta').format(name, item_type)
            raise Exception(msg)

        details = "(no additional details)"
        if "details" in meta:
            details = meta["details"]

        # build generic meta
        data = [
            ['Title', meta['title']],
            ['Name', meta['name']],
            ['Version', str(meta['version'])],
        ]


        if item_type == 'raster':

            colnames_list =  [
                '{0}.{1}.{2}'.format(dset['name'], i, j)
                for i in [f['name'].split('_')[-1] for f in dset['files']]
                for j in dset['options']['extract_types']
            ]

            colnames = ('Format: "{0}.&lt;temporal&gt;.&lt;method&gt;" <br /> '
                        'for all combinations of &lt;temporal&gt; and &lt;method&gt; '
                        'which can be found in the "Temporal Selection" and '
                        '"Extract Types Selected" fields below '
                        '({1} columns total)').format(
                            dset['name'], len(colnames_list)
                        )

            data.append(['Column Names ', colnames])

            # data.append(['Temporal Type', dset['temporal_type']])

            temporal_raw = [f['name'].split('_')[-1] for f in dset['files']]

            if 'none' in temporal_raw:
                temporal_str = temporal_raw
            else:
                temporal_int = [int(s) for s in temporal_raw]
                temporal_sorted = sorted(temporal_int, reverse=True)
                temporal_str = [str(ts) for ts in temporal_sorted]

            data.append(['Temporal Selection', ', '.join(temporal_str)])

            data.append(['Extract Types Selected', ', '.join([
                "{0} ({1})".format(i, meta['options']['extract_types_info'][i])
                for i in dset['options']['extract_types']
            ])])


        elif item_type == 'release':

            colnames = ', '.join([
                '{0}.<br />    {1}.<br />    {2}'.format(dset['dataset'], dset['hash'][0:7], i)
                for i in ['sum', 'potential', 'reliability']
            ])

            ###
            if dset['dataset'].startswith('worldbank'):
                colnames = '{0}.<br />    {1}.<br />    {2}'.format(dset['dataset'], dset['hash'][0:7], 'sum')
            ###

            data.append(['Column Names ', colnames])
            data.append([pg('<b>Filters</b>', 1), 'hash: {0}'.format(dset['hash'])])

            for f in dset['filters']:
                try:
                    data.append([f, ', '.join([str(i) for i in dset['filters'][f]])])
                except:
                    data.append([f, ', '.join([i.encode('ascii', 'ignore') for i in dset['filters'][f]])])



        data.append(['',''])
        data.append(['Description', meta['description']])
        data.append(['Details', details])

        # data.append(['Type', meta['type']])
        # data.append(['File Format', meta['file_format']])
        # data.append(['File Extension', meta['file_extension']])
        # data.append(['Scale', meta['scale']])
        # data.append(['Temporal', ''])


        # data.append(['Temporal Type', meta['temporal']['name']])

        # if meta['temporal']['format'] != 'None':
        #     data.append(['Temporal Format', meta['temporal']['format']])
        #     data.append(['Temporal Start', str(meta['temporal']['start'])])
        #     data.append(['Temporal End', str(meta['temporal']['end'])])

        data.append(['Bounding Box', str(meta['spatial']['coordinates'])])

        data.append(['Date Added', str(meta['asdf']['date_added'])])
        data.append(['Date Updated', str(meta['asdf']['date_updated'])])

        if 'sources_name' in meta['extras']:
            data.append(['Source Name', meta['extras']['sources_name']])

        def enforce_max_word_length(string, max_chars=80):
            raw_word_list = string.split(" ")
            short_word_list = []
            for word in raw_word_list:
                if len(word) > max_chars:
                    split_word = [
                        word[i:i+max_chars]
                        for i in range(0, len(word), max_chars)
                    ]
                    fixed_word = "\n".join(split_word)
                else:
                    fixed_word = word
                short_word_list += [fixed_word]
            return " ".join(short_word_list)

        if 'sources_web' in meta['extras']:
            tmp_sources_web = meta['extras']['sources_web']
            tmp_sources_web = enforce_max_word_length(tmp_sources_web)
            data.append(['Source Link', tmp_sources_web])

        if 'citation' in meta['extras']:
            tmp_citation = meta['extras']['citation']
            tmp_citation = enforce_max_word_length(tmp_citation)
            data.append(['Citation', tmp_citation])


        if item_type == 'boundary':
            pass
            # data.append(['Group', meta['options']['group']])
            # data.append(['Group Class', meta['options']['group_class']])
            # data.append(['Group Title', meta['options']['group_title']])

        elif item_type == 'raster':
            data.append(['Variable Description', meta['options']['variable_description']])
            data.append(['Resolution', str(meta['options']['resolution'])])
            # data.append(['Extract Types', ', '.join(meta['options']['extract_types'])])
            data.append(['Factor', str(meta['options']['factor'])])

        elif item_type == 'release':
            download_link = 'http://aiddata.org/datasets'
            # download_link = 'https://github.com/AidData-WM/public_datasets/tree/master/geocoded' #+ meta['data_set_preamble'] +'_'+ meta['data_type'] +'_v'+ str(meta['version']) + '.zip'
            data.append(['Download Link', download_link])

        return data


    def add_meta(self):

        ptext = '<b><font size=14>Meta Information</font></b>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.25*inch))

        # full boundary meta
        ptext = '<font size=10><b>Boundary</b></font>'
        self.Story.append(Paragraph(ptext, self.styles['Normal']))
        self.Story.append(Spacer(1, 0.05*inch))


        # build boundary meta table array
        data = self.build_meta(self.request['boundary']['name'], 'boundary', None)

        data = [[i[0], pg(i[1], 2)] for i in data]
        t = Table(data)
        t.setStyle(TableStyle([
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black)
        ]))

        self.Story.append(t)
        self.Story.append(Spacer(1, 0.25*inch))


        # full dataset meta

        meta_log = []
        for dset in self.request['release_data']:

            if dset['dataset'] not in meta_log:
                meta_log.append(dset['dataset'])

                ptext = '<font size=10><b>Dataset {0} - {1}</b></font>'.format(
                    len(meta_log), dset['custom_name'])

                self.Story.append(Paragraph(ptext, self.styles['Normal']))
                self.Story.append(Spacer(1, 0.05*inch))

                # build dataset meta table array
                data = self.build_meta(dset['dataset'], 'release', dset)

                data = [[i[0], pg(i[1], 2)] for i in data]
                t = Table(data)
                t.setStyle(TableStyle([
                    ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
                    ('BOX', (0,0), (-1,-1), 0.25, colors.black)
                ]))

                self.Story.append(t)
                self.Story.append(Spacer(1, 0.25*inch))


        for dset in self.request['raster_data']:

            if dset['name'] not in meta_log:
                meta_log.append(dset['name'])

                ptext = '<font size=10><b>Dataset {0} - {1}</b></font>'.format(
                    len(meta_log), dset['custom_name'])
                self.Story.append(Paragraph(ptext, self.styles['Normal']))
                self.Story.append(Spacer(1, 0.05*inch))

                # build dataset meta table array
                data = self.build_meta(dset['name'], dset['type'], dset)

                data = [[i[0], pg(i[1], 2)] for i in data]
                t = Table(data)
                t.setStyle(TableStyle([
                    ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
                    ('BOX', (0,0), (-1,-1), 0.25, colors.black)
                ]))

                self.Story.append(t)
                self.Story.append(Spacer(1, 0.25*inch))





    # license stuff
    def add_additional(self):

        with open(self.assets_dir + '/templates/additional.txt') as license:
            for line in license:
                p = Paragraph(line, self.styles['BodyText'])
                self.Story.append(p)




    # write the document to disk
    def output_doc(self):
        self.doc.build(self.Story)

