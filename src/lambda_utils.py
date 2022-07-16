
def getParameter( param_dict, k, v_default ):
    """ Return value of key k in param_dict, if found - otherwise return v_default.
    """
    if k in param_dict:
        return param_dict[k]
    else:
        return v_default

