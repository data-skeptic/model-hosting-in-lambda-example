import copy
from datetime import datetime
from decimal import Decimal
import furl
from importlib import import_module
from io import BytesIO, StringIO
import jsons
from pathlib import Path


def clean_json_arr(jarr):
    result = []
    for item in jarr:
        clean = clean_json_dict(item)
        result.append(clean)
    return result


def clean_json_dict(jdict):
    if type(jdict) == int:
        return jdict
    if type(jdict).__name__ == 'int64':
        return int(jdict)
    if type(jdict) == str:
        return jdict
    if type(jdict) == float:
        return Decimal(str(jdict))
    if type(jdict) == Decimal:
        return jdict
    if type(jdict) == bool:
        return jdict
    if type(jdict) == datetime:
        return str(jdict)
    result = {}
    if type(jdict) == list:
        return clean_json_arr(jdict)
    for col in jdict.keys():
        v = jdict[col]
        dirty_filthy_nasty_columnsies = ['cookies', 'headers']
        if col in dirty_filthy_nasty_columnsies:
            if v == '' or v is None:
                pass
            elif col==col:
                result[f'{col}_str'] = jsons.dumps(v)
            else:
                result[col] = v
        else:
            typ = type(v)
            if typ == dict:
                if len(v) > 0:
                    result[col] = clean_json_dict(v)
            elif typ == Decimal:
                result[col] = float(v)
            #elif typ == int64:
            #    result[col] = int(v)
            elif typ == tuple:
                result[col] = list(v)
            elif typ == str:
                if v.strip() != '':
                    result[col] = v
            elif typ == int:
                result[col] = v
            elif typ == float:
                result[col] = Decimal(str(v))
            elif v is not None:
                cval = clean_json_dict(v)
                if cval is not None:
                    result[col] = cval
    return result


def match(first, second):
    # If we reach at the end of both strings, we are done
    if len(first) == 0 and len(second) == 0:
        return True
    # Make sure that the characters after '*' are present
    # in second string. This function assumes that the first
    # string will not contain two consecutive '*'
    if len(first) > 1 and first[0] == '*' and  len(second) == 0:
        return False
    # If the first string contains '?', or current characters
    # of both strings match
    if (len(first) > 1 and first[0] == '?') or (len(first) != 0
        and len(second) != 0 and first[0] == second[0]):
        return match(first[1:],second[1:]);
    # If there is *, then there are two possibilities
    # a) We consider current character of second string
    # b) We ignore current character of second string.
    if len(first) !=0 and first[0] == '*':
        return match(first[1:],second) or match(first,second[1:])
    return False


def dataframe2parquet_bytes(df):
    raise Exception("Use local branch instead")
    if df is None or df.shape[0] == 0:
        return None
    d = dict(zip(df.columns, df.dtypes))
    for k in d.keys():
        v = d[k].__class__.__name__
        if v == 'float64':
            df[k].fillna(0, inplace=True)
        elif v == 'bool':
            df[k] = df[k].astype(int)
        else:
            df[k] = df[k].fillna('')
    try:
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, engine='pyarrow')
    except:
        # TODO: fix this awful technical debt
        print('try harder')
        out_buffer = _try_harder(df)
    out_buffer.seek(0)
    bytez = out_buffer.read()
    return bytez


def get_domain(uri, furl_dict=None):
    if uri.find('://') == -1:
        uri = f'http://{uri}'
    if furl_dict is None:
        try:
            furl_dict = furl.furl(uri).asdict()
        except:
            raise Exception(f'Invalid URL: {uri}')
    domain = furl_dict['host']
    if domain is None:
        raise Exception(f'Invalid URL: {uri}')
    if domain.startswith('www.'):
        domain = domain[4:]
    arr = domain.split('.')
    arr.reverse()
    return '.'.join(arr)

def analyze_uri(uri):
    he = uri.find('://')
    if he == -1:
        schema = None
        he = -3
    else:
        schema = uri[:he]
    pe = uri.rfind(':')
    if pe == -1:
        port = None
        pe = len(uri)
    else:
        port = int(uri[pe+1:])
    host = uri[he+3:pe]
    """
    Get Host, Port, Schema from Uri.
    ex: uri = https://test.domain.com:2345
    will return dict
    {
        host: 'test.domain.com',
        port: '2345',
        schema: 'https'
    }
    """
    return {
        'host': host,
        'port': port,
        'schema': schema
    }


def get_extension(key, ext='.crawl'):
    if key is None:
        return None
    # h = key.find('://')
    # if h != -1:
    #     return ext
    i = key.rfind('/')
    j = key.find('.', i)
    ext = key[j:]
    return ext.lower()


def get_folder(key):
    if key is None:
        return None
    i = key.rfind('/') + 1
    return key[0:i]


def get_username(key):
    return key.split('/')[1]
    

def get_stem(key):
    return Path(key).stem


def change_extension(key, ext):
    ext2 = get_extension(key)
    i = key.rfind(ext2)
    return key[0:i] + ext


def extension_match(ext, ext_pattern):
    if ext_pattern == '.*':
        return True
    if ext == ext_pattern:
        return True
    if ext.endswith(ext_pattern):
        return True
    return False


def filter_dict(old_dict, your_keys):
    return { your_key: old_dict[your_key] for your_key in your_keys }


# def build_parser(name, Klass: AbstractParser, key: str, dest_ext=None):
#     parser_name = f'{Klass.__module__}.{Klass.__name__}'
#     if dest_ext is None:
#         dest_ext = Klass.get_output_extension()
#     metadata_item = ParseRequest(Klass.__name__, parser_name, key, dest_ext)
#     return metadata_item


def build_request(class_full_name: str, kwargs: dict):
    try:
        module_path, class_name = class_full_name.rsplit('.', 1)
        module = import_module(module_path)
        Request = getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        print('Import error:', class_full_name)
        raise ImportError(class_full_name)
    r = Request(**kwargs)
    return r


def work_from_dict(work_dict):
    class_full_name = work_dict['type']
    # TODO: need a better system.  This is "hidden" to a new dev
    #chalicelib.applications.texttools.de_index_request.DeIndexRequest
    work = build_request(class_full_name, work_dict)
    return work


def handler_from_dict(work_dict: dict, app_manager):
    if 'parser_name' in work_dict:
        class_full_name = work_dict['parser_name']
    else:
        class_full_name = work_dict['type']
        kwargs = work_dict
    if class_full_name.endswith('.Request'):
        class_full_name = class_full_name[0:-8] + '.Handler'
        kwargs = copy.deepcopy(work_dict)
        kwargs['app_manager'] = app_manager
    handler = build_request(class_full_name, kwargs)
    return handler


def get_es_doc_address(application_name, extension, dest_key):
    return {
        "index": f"feaas",
        "doc_type": extension,
        "id": dest_key
    }
def get_common_image_extensions() -> list:
    return [".jpeg", ".jpg", ".png"]

def get_common_text_extensions() -> list:
    return [".text", ".txt", ".csv"]

