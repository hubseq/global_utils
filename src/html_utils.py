import os, sys
#import plotly.express as px
#import plotly.figure_factory as ff
#import matplotlib.pyplot as plt
from io import BytesIO
import base64

def add_image_to_html( p, p_type, t = '' ):
    """ Given <plot_figure> and <plot_type>, returns an img src tag with embedded image
    Types:
    'plotly' : plotly figure is passed as p
    'matplotlib': MPL figure is passed as p
    'image' : image file name is passed as p

    t is an optional header title for the image
    """
    img_tag = ''    
    # add optional title
    if t != '':
        img_tag += '<h5>{}</h5>'.format(t)

    # add image        
    if p_type in ['plotly', 'matplotlib']:
        imgfile = BytesIO()
        if p_type == 'plotly':
            p.write_image(imgfile, format='png') # matplotlib is savefig
        elif p_type == 'matplotlib':
            p.savefig(imgfile)
        encoded = base64.b64encode(imgfile.getvalue()).decode('utf-8')
        img_tag += '<img src=\'data:image/png;base64,{}\'>'.format(encoded) + '<br>'
    else: # pure image
        encoded = base64.b64encode(open(p, 'rb').read()).decode('utf-8')
        img_tag += '<img src=\'data:image/png;base64,{}\'>'.format(encoded) + '<br>'        
    return img_tag


def plots_to_html( plots_object_list, html_outname ):
    """ Given a list of plot objects (plotly or matplotlib), outputs these plots to an HTML
    Example: plots_to_html( [[plot_pca2d, 'plotly'], [plot_pca3d, 'plotly', 'title - PCA'], ['myimage.png', 'image']], 'plots.html'))
    """
    img_tags = ''
    with open(html_outname,'w') as f:
        for p_tuple in plots_object_list:
            img_tags += add_image_to_html( p_tuple[0], p_tuple[1], p_tuple[2] if len(p_tuple) > 2 else '' )
    html = '<html><body>' + img_tags + '</body></html>'
    with open( html_outname, 'w' ) as f:
        f.write(html)
    return
