import copy
def fix_dict(datax, ignore_duplicate_key=False):
    """
    Removes dots "." from keys, as mongo doesn't like that.
    If the key is already there without the dot, the dot-value get's lost.
    This modifies the existing dict!

    :param ignore_duplicate_key: True: if the replacement key is already in the dict, now the dot-key value will be ignored.
                                 False: raise ValueError in that case.
    """
    #datax = copy.deepcopy(data)
    
    if isinstance(datax, (list, tuple)):
        list2 = list()
        for e in datax:
            list2.append(fix_dict(e))
        # end if
        return list2
    if isinstance(datax, dict):
        # end if
        for key, value in datax.items():
            value = fix_dict(value)
            old_key = key
            if "." in key:
                key = old_key.replace(".", "_")
                #if key not in datax:
                datax[key] = value
                # else:
                #     error_msg = "Dict key {key} containing a \".\" was ignored, as {replacement} already exists".format(
                #         key=old_key, replacement=key)
                #     # if force:
                #     import warnings
                #     warnings.warn(error_msg, category=RuntimeWarning)
                #     # else:
                #     #     raise ValueError(error_msg)
                #     # end if
                # end if
                del datax[old_key]
            # end if
            datax[key] = value
        # end for
        return datax
    # end if
    return datax
# end def
