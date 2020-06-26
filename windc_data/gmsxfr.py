import os
import copy
import pandas as pd
import numpy as np
import networkx as nx
import time
import matplotlib as mpl
import matplotlib.pyplot as plt
from schema import Schema, And, Or, Use, Optional
import sys
from tqdm import tqdm

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

try:
    from gdxcc import *
except ImportError:
    raise ImportError(
        "GAMS python api is not currently installed... must be manually installed."
    )


class GdxContainer:
    """
    A container object that can be used to rapidly load data from an external GAMS GDX file. The GdxContainer enables the translation between GDX and several other popular data structures such as native Python dictionaries and Pandas dataframes. The GdxContainer requires that the GAMS Python API be installed. Upon construction GdxContainer will scan the GDX file and pull out important data to aid in the reading of GDX symbols.

    Parameters
    ----------
    gdxfilename : string, required. Relative or absolute path name of the GDX file to read from. User based paths (~/) are translated into absolute paths. GdxContainer will check if file has the '.gdx' extension and if not, raise an exception. If file is not found in path, an exception is also raised.

    gams_sysdir : string, required. Absolute path of the GAMS sysdir. Used to import GDX DLL files for reading of the GDX.

    Examples
    --------
    Constructing a GdxContainer.
    >>> gdxin = gdxrw_mod.GdxContainer(gdxfilename='~/path/to/gdxfile/example.gdx', gams_sysdir='/Applications/GAMS30.2/Resources/sysdir')

    >>> gdxin.syms  # get all symbols in GDX (list)
    >>> gdxin.symDim  # get dimensionality for all symbols in GDX (dict)
    >>> gdxin.symDomain  # get domain information for all symbols in GDX (dict)
    >>> gdxin.symNrRecs  # get number of records for all symbols in GDX (dict)
    >>> gdxin.symText  # get descriptive text for all symbols in GDX (dict)
    >>> gdxin.symType  # get type for all symbols in GDX (dict)

    """

    def __init__(self, gams_sysdir, gdxfilename=None):
        """ __init__ constructor """

        if os.path.isabs(gams_sysdir) == False:
            raise Exception(
                "must enter in full (absolute) path to GAMS sysdir directory"
            )

        self.gams_sysdir = gams_sysdir

        # set up empty gdx data containers
        self.syms = []
        self.symType = {}
        self.symDim = {}
        self.symNrRecs = {}
        self.symElements = {}
        self.symDomain = {}
        self.symText = {}
        self.symAliasWith = {}
        self.symDomainInfoType = {}
        self.__symSource__ = {}  # 0 = gdx file, 1 = user specified
        self.__gdxSpecialValues__ = {}
        self.__symName__ = {}
        self.__symGdxIdx__ = {}
        self.__strUels__ = {}
        self.__symLoaded__ = []
        self.__setText__ = {}
        self.__record_attr__ = {"L", "M", "LO", "UP", "SCALE"}
        self.__all_data_types__ = {
            "scalar",
            "parameter",
            "set",
            "singleton_set",
            "variable",
            "equation",
            "alias",
        }
        self.__domain_info_types__ = {"none", "relaxed", "regular"}
        self.__std_keys__ = {
            "type",
            "dimension",
            "domain",
            "number_records",
            "text",
            "elements",
            "domain_info",
            "alias_with",
        }
        # test DLL connection and create gdxHandle
        try:
            self.__create_gdxHandle__()
            ret, fileVersion, producer = gdxFileVersion(self.__gdxHandle__)
            self.gamsVersion = gdxGetDLLVersion(self.__gdxHandle__)[1]
            assert not gdxClose(self.__gdxHandle__)
        except:
            raise Exception("Could not properly load GDX DLL, check directory path")

        # set special values
        self.__set_gdx_specVals__()

        # gdx filename path handling
        if gdxfilename is not None:
            if gdxfilename[-3:] != "gdx":
                raise Exception("check filename extension, must be gdx")

            if os.path.isabs(gdxfilename) == False:
                gdxfilename = os.path.abspath(gdxfilename)

            self.gdxfilename = gdxfilename

            self.__read_basic_gdx_features__()

            for i in self.syms:
                self.__symSource__[i] = 0  # set source of the symbol as a gdx file
        else:
            self.gdxfilename = None

    def __create_gdxHandle__(self):
        self.__gdxHandle__ = new_gdxHandle_tp()
        rc = gdxCreateD(self.__gdxHandle__, self.gams_sysdir, GMS_SSSIZE)
        assert rc[0], rc[1]

    def __open_file_for_reading__(self):
        rc = gdxOpenRead(self.__gdxHandle__, self.gdxfilename)
        assert rc
        ret, self.__sycnt__, self.__uelcnt__ = gdxSystemInfo(self.__gdxHandle__)

    def __set_gdx_specVals__(self):
        # map special values and gdx types (type, subtype, dimension)
        self.__py2gdxtypemap__ = {
            "set": (GMS_DT_SET, 0, -1),
            "parameter": (GMS_DT_PAR, 0, -1),
            "singleton_set": (GMS_DT_SET, 1, -1),
            "variable": (GMS_DT_VAR, 0, -1),
            "equation": (GMS_DT_EQU, 0, -1),
            "alias": (GMS_DT_ALIAS, 0, -1),
            "scalar": (GMS_DT_PAR, 0, 0),
        }

        self.__gdx2pytypemap__ = {
            (GMS_DT_SET, 0, -1): "set",
            (GMS_DT_PAR, 0, -1): "parameter",
            (GMS_DT_SET, 1, -1): "singleton_set",
            (GMS_DT_VAR, 0, -1): "variable",
            (GMS_DT_EQU, 0, -1): "equation",
            (GMS_DT_ALIAS, 0, -1): "alias",
            (GMS_DT_PAR, 0, 0): "scalar",
        }

        # get special values from gdx
        specVals = doubleArray(GMS_SVIDX_MAX)
        gdxGetSpecialValues(self.__gdxHandle__, specVals)
        self.__gdxSpecialValuesReadAs__ = {
            specVals[GMS_SVIDX_UNDEF]: np.nan,
            specVals[GMS_SVIDX_NA]: np.nan,
            specVals[GMS_SVIDX_PINF]: np.inf,
            specVals[GMS_SVIDX_MINF]: np.NINF,
            specVals[GMS_SVIDX_EPS]: np.finfo(float).tiny,
        }

        self.__map_to_numpy_float__ = {
            "undef": np.nan,
            "UNDEF": np.nan,
            "Undef": np.nan,
            "undefined": np.nan,
            "Undefined": np.nan,
            "UNDEFINED": np.nan,
            "eps": np.finfo(float).tiny,
            "EPS": np.finfo(float).tiny,
            "Eps": np.finfo(float).tiny,
            "epsilon": np.finfo(float).tiny,
            "Epsilon": np.finfo(float).tiny,
            "EPSILON": np.finfo(float).tiny,
        }

        # no robust mapping from np.nan to UNDEF, mapping to NA instead
        self.__gdxSpecialValuesWriteAs__ = {
            # np.nan: specVals[GMS_SVIDX_UNDEF],
            np.nan: specVals[GMS_SVIDX_NA],
            np.inf: specVals[GMS_SVIDX_PINF],
            np.NINF: specVals[GMS_SVIDX_MINF],
            np.finfo(float).tiny: specVals[GMS_SVIDX_EPS],
        }

    def __read_basic_gdx_features__(self):
        # open file for reading
        self.__open_file_for_reading__()

        # get any set text that might exist
        ret = 1
        txtnr = 0
        while ret != 0:
            txtnr += 1
            ret, string, n = gdxGetElemText(self.__gdxHandle__, txtnr)

            if ret != 0:
                self.__setText__[txtnr] = string

        # note that this is in symbol indexing (skips universe)
        for i in range(1, self.__uelcnt__ + 1):
            ret, uelstr, uelmap = gdxUMUelGet(self.__gdxHandle__, i)
            self.__strUels__[i] = uelstr

        for i in range(self.__sycnt__ + 1):
            ret, syid, dimen, typ = gdxSymbolInfo(self.__gdxHandle__, i)
            synr, nrecs, subtype, expltxt = gdxSymbolInfoX(self.__gdxHandle__, i)

            self.syms.append(syid)
            self.__symName__[i] = syid
            self.__symGdxIdx__[syid] = i
            self.symDim[syid] = dimen
            self.symText[self.__symName__[i]] = expltxt

            # need to adjust values to capture special cases
            if typ != GMS_DT_SET:
                subtype = 0.0

            if typ == GMS_DT_PAR and dimen == 0.0:
                dimen = 0.0
            else:
                dimen = -1.0

            self.symType[syid] = self.__gdx2pytypemap__[(typ, subtype, dimen)]

            if self.symType[syid] == "alias":
                self.symAliasWith[syid] = expltxt.split("Aliased with ")[1]
            else:
                self.symAliasWith[syid] = None

            ret, nrRecs = gdxDataReadRawStart(self.__gdxHandle__, i)
            self.symNrRecs[syid] = nrRecs

            ret, domain = gdxSymbolGetDomainX(self.__gdxHandle__, i)
            self.symDomain[self.__symName__[i]] = domain

            dc_map = {0: "none", 1: "none", 2: "relaxed", 3: "regular"}
            self.symDomainInfoType[self.__symName__[i]] = dc_map[ret]

            self.symDomain["*"] = ["*"]  # override default GAMS API return

        # close file
        assert not gdxClose(self.__gdxHandle__)

    def rgdx(self, sym=None):
        """
        The rgdx() method is the primary method to load data from the user specified GDX file. Upon construction, GdxContainer will scan the GDX file to collect basic symbol information, but it will not retrieve symbol data; rgdx() must be used to load the symbol into memory. Symbol data is stored in self.symElements.

        Parameters
        ----------
        sym : string or list, not required. Defaults to reading all symbols into memory if input is NoneType. Only the name of the symbol is required, rgdx() will understand data formats for GAMS types -- set, parameter, alias, singleton set, scalar, equation, and variable. rgdx() check to make sure that sym is in the GDX file otherwise exceptions are raised. If the symbol has previous been loaded rgdx() will overwrite with a warning, but no exception is raised.

        Returns
        -------
        NoneType


        Examples
        --------
        Read in all symbols into the GdxContainer from the gdx file that was specified (self.gdxfilename).
        >>> gdxin.rgdx()

        Read in a single symbol from the GDX file.
        >>> gdxin.rgdx(sym='i')

        Read in a subset of symbols from the GDX file.
        >>> gdxin.rgdx(sym=['i','j','x'])

        Once symbols are loaded into the GdxContainer object a list of those symbols can be retrieved with self.__symLoaded__.
        """
        if sym is None:
            sym = self.syms

        if isinstance(sym, str):
            sym = [sym]

        for n in sym:
            # check if sym is a gdx symbol
            if n not in self.syms:
                raise Exception(f"{n} is not in the GDX file, check spelling?")

            # check to see if already loaded, if yes, return warning, but do not abort
            if n in self.__symLoaded__:
                print(f'symbol "{n}" alredy loaded into memory, overwriting...')

            if n not in self.__symLoaded__:
                self.__symLoaded__.append(n)

            # open file for reading
            self.__open_file_for_reading__()

            # data read start
            ret, nrRecs = gdxDataReadRawStart(self.__gdxHandle__, self.__symGdxIdx__[n])

            # # read data into a structured numpy array
            # elements_c = gdx2np.readSymbol(self.gdxfilename, n, False)
            # print(elements_c)

            elements = []
            set_element_text = {}
            for _ in range(self.symNrRecs[n]):
                ret, e, v, a = gdxDataReadRaw(self.__gdxHandle__)

                # map in special values
                v = [
                    self.__gdxSpecialValuesReadAs__[i]
                    if i in self.__gdxSpecialValuesReadAs__.keys()
                    else i
                    for i in v
                ]
                e.extend(v)
                elements.append(tuple(e))

            # formulate dtypes
            dts = [(f"dim{i}", "int") for i in range(self.symDim[n])]
            dts.extend(
                [
                    ("L", "float"),
                    ("M", "float"),
                    ("LO", "float"),
                    ("UP", "float"),
                    ("SCALE", "float"),
                ]
            )

            self.symElements[n] = np.array(elements, dtype=dts)

            # if not self.symDomain[n] or set(self.symDomain[n]) == {'*'}:
            #     self.symDomainCheck[n] = False
            # else:
            #     self.symDomainCheck[n] = True

            gdxDataReadDone(self.__gdxHandle__)

        # close file
        assert not gdxClose(self.__gdxHandle__)

    def to_dict(self, sym=None, fields="L", uel_idx=False):
        """
        The to_dict method is used to format the data loaded in the GdxContainer into a python dict.  All metadata and text descriptions are included in this structure. For sets -- symbol elements are stored as a list or a list of tuples.  For paramters, equations, and variables -- symbol elements are formatted as a dictionary where the keys are either int, string, or tuples, values are type float. Special values are mapped to common numpy types.

        Parameters
        ----------
        sym : string or list, not required. Defaults to all loaded symbols if input is NoneType. If an element in sym is not in self.__symLoaded__ nothing happens, the ValueError is simply skipped but a warning is issued.

        fields : string or list, not required. Values can be 'L', 'M', 'LO', 'UP', or 'SCALE' or a list of any combination. Default is to format only 'L' (level) values from GAMS into the final dict. Fields are ignored for GAMS sets.

        uel_idx : bool, not required. Default is False. If False, the final dict will have the string mapped into the domain of the GDX symbol. If True, the original UEL indexing is left intact.  UEL indexing can save on memory requirements, important when working with large symbols.  The uel indexing can be accessed with self.__strUels__

        Returns
        -------
        dict


        Examples
        --------
        Format all loaded data into a dict.
        >>> gdxin.rgdx(sym=['i','x'])
        >>> gdxin.to_dict()
        {'i': {'type': 'set',
          'dimension': 1,
          'domain': ['*'],
          'number_records': 2,
          'text': 'canning plants',
          'elements': ['seattle', 'san-diego']},
         'x': {'type': 'var',
          'dimension': 2,
          'domain': ['i', 'j'],
          'number_records': 6,
          'text': 'shipment quantities in cases',
          'elements': {('seattle', 'new-york'): 50.0,
           ('seattle', 'chicago'): 300.0,
           ('seattle', 'topeka'): 0.0,
           ('san-diego', 'new-york'): 275.0,
           ('san-diego', 'chicago'): 0.0,
           ('san-diego', 'topeka'): 275.0}}}

        >>> gdxin.to_dict(uel_idx=True)
        {'i': {'type': 'set',
          'dimension': 1,
          'domain': ['*'],
          'number_records': 2,
          'text': 'canning plants',
          'elements': [1, 2]},
         'x': {'type': 'var',
          'dimension': 2,
          'domain': ['i', 'j'],
          'number_records': 6,
          'text': 'shipment quantities in cases',
          'elements': {(1, 3): 50.0,
           (1, 4): 300.0,
           (1, 5): 0.0,
           (2, 3): 275.0,
           (2, 4): 0.0,
           (2, 5): 275.0}}}

        >>>gdxin.to_dict(uel_idx=False, fields=['L','M'])
        {'i': {'type': 'set',
          'dimension': 1,
          'domain': ['*'],
          'number_records': 2,
          'text': 'canning plants',
          'elements': ['seattle', 'san-diego']},
         'x': {'type': 'var',
          'dimension': 2,
          'domain': ['i', 'j'],
          'number_records': 6,
          'text': 'shipment quantities in cases',
          'elements': {('seattle', 'new-york'): {'L': 50.0, 'M': 0.0},
           ('seattle', 'chicago'): {'L': 300.0, 'M': 0.0},
           ('seattle', 'topeka'): {'L': 0.0, 'M': 0.036},
           ('san-diego', 'new-york'): {'L': 275.0, 'M': 0.0},
           ('san-diego', 'chicago'): {'L': 0.0, 'M': 0.009},
           ('san-diego', 'topeka'): {'L': 275.0, 'M': 0.0}}}}
        """
        if sym is None:
            sym = self.__symLoaded__

        if isinstance(sym, str):
            sym = [sym]

        if isinstance(fields, str):
            fields = [fields]

        if not isinstance(uel_idx, bool):
            raise Exception("uel_idx must be type bool")

        if not isinstance(fields, list):
            raise Exception("fields must be type str or list")

        if not isinstance(sym, list):
            raise Exception("sym must be type str or list")

        if uel_idx == True and set(self.__symSource__.values()) == {1}:
            print(
                "WARNING: uel_idx set to True, but data in GdxContainar cannot support uel_idx, ignoring..."
            )
            uel_idx = False

        df = {}
        for n in sym:
            if n in self.__symLoaded__:

                df[n] = {}
                df[n]["type"] = self.symType[n]
                df[n]["dimension"] = self.symDim[n]
                df[n]["domain"] = self.symDomain[n]
                df[n]["domain_info"] = self.symDomainInfoType[n]
                df[n]["alias_with"] = self.symAliasWith[n]
                df[n]["number_records"] = self.symNrRecs[n]
                df[n]["text"] = self.symText[n]

                # get data
                elements = pd.DataFrame.from_records(self.symElements[n])

                # drop attr columns if necessary
                if self.symType[n] in {"set", "singleton_set", "alias"}:
                    elements.drop(columns=self.__record_attr__, inplace=True)
                else:
                    elements.drop(
                        columns=self.__record_attr__ - set(fields), inplace=True
                    )

                if uel_idx == False:
                    if self.__symSource__[n] == 0:
                        for field in ["dim" + str(j) for j in range(self.symDim[n])]:
                            elements[field] = elements[field].map(self.__strUels__)
                            elements[field] = elements[field].map(str)

                # create python data types
                if self.symType[n] in {"set", "alias", "singleton_set"}:
                    if self.symDim[n] == 1:
                        df[n]["elements"] = list(elements["dim0"])
                    if self.symDim[n] > 1:
                        df[n]["elements"] = list(elements.to_records(index=False))

                if self.symType[n] in {"parameter", "scalar"}:
                    if self.symDim[n] == 0:
                        df[n]["elements"] = elements["L"][0]

                    if self.symDim[n] == 1:
                        df[n]["elements"] = dict(zip(elements["dim0"], elements["L"]))

                    if self.symDim[n] > 1:
                        domain = list(
                            elements[["dim" + str(j) for j in range(self.symDim[n])]]
                            .to_records(index=False)
                            .tolist()
                        )

                        values = list(elements["L"])

                        df[n]["elements"] = dict(zip(domain, values))

                if self.symType[n] in {"variable", "equation"}:
                    if self.symDim[n] == 0:
                        df[n]["elements"] = elements["L"][0]

                    if self.symDim[n] == 1:
                        domain = list(elements["dim0"])

                        values = [
                            {i: elements.loc[k, i] for i in fields}
                            for k in range(self.symNrRecs[n])
                        ]

                        df[n]["elements"] = dict(zip(domain, values))

                    if self.symDim[n] > 1:
                        domain = list(
                            elements[["dim" + str(j) for j in range(self.symDim[n])]]
                            .to_records(index=False)
                            .tolist()
                        )

                        values = [
                            {i: elements.loc[k, i] for i in fields}
                            for k in range(self.symNrRecs[n])
                        ]

                        df[n]["elements"] = dict(zip(domain, values))

            else:
                print(
                    f'WARNING: symbol "{n}" not currently loaded into memory, ignoring.'
                )

        if len(sym) == 1:
            return df[sym[0]]
        else:
            return df

    def to_dataframe(self, sym=None, fields="L", uel_idx=False):
        """
        The to_dataframe method is used format the data loaded in the GdxContainer into a pandas dataframe.  Metadata and text descriptions are not included in this structure. Structure resembles the to_dict(). Special values are mapped to common numpy types.

        Parameters
        ----------
        sym : string or list, not required. Defaults to all loaded symbols if input is NoneType. If an element in sym is not in self.__symLoaded__ nothing happens, the ValueError is simply skipped and a warning is issued. When multiple symbols are included in sym as a list, to_dataframe() returns a dict of dataframes.

        fields : string or list, not required. Values can be 'L', 'M', 'LO', 'UP', or 'SCALE' or a list of any combination. Default is to format only 'L' (level) values from GAMS into the final dict. Fields are ignored for GAMS sets.

        uel_idx : bool, not required. Default is False. If False, the final dict will have the string mapped into the domain of the GDX symbol. If True, the original UEL indexing is left intact.  UEL indexing can save on memory requirements, important when working with large symbols.  The uel indexing can be accessed with self.__strUels__

        Returns
        -------
        dict or dataframe


        Examples
        --------
        Format all loaded data into a dict.
        >>> gdxin.rgdx(sym=['i','x'])
        >>> gdxin.to_dataframe()
            {'i':            *
                     0    seattle
                     1  san-diego,
             'x':            i         j      L
                     0    seattle  new-york   50.0
                     1    seattle   chicago  300.0
                     2    seattle    topeka    0.0
                     3  san-diego  new-york  275.0
                     4  san-diego   chicago    0.0
                     5  san-diego    topeka  275.0}

        >>> gdxin.to_dataframe(uel_idx=True)
            {'i':    *
                 0  1
                 1  2,
             'x':    i  j      L
                 0  1  3   50.0
                 1  1  4  300.0
                 2  1  5    0.0
                 3  2  3  275.0
                 4  2  4    0.0
                 5  2  5  275.0}

        >>>gdxin.to_dataframe(uel_idx=False, fields=['L','M'])
            {'i':            *
                     0    seattle
                     1  san-diego,
             'x':            i         j      L      M  SCALE
                     0    seattle  new-york   50.0  0.000    1.0
                     1    seattle   chicago  300.0  0.000    1.0
                     2    seattle    topeka    0.0  0.036    1.0
                     3  san-diego  new-york  275.0  0.000    1.0
                     4  san-diego   chicago    0.0  0.009    1.0
                     5  san-diego    topeka  275.0  0.000    1.0}
        """

        if sym is None:
            sym = self.__symLoaded__

        if isinstance(sym, str):
            sym = [sym]

        if isinstance(fields, str):
            fields = [fields]

        if not isinstance(uel_idx, bool):
            raise Exception("uel_idx must be type bool")

        if not isinstance(fields, list):
            raise Exception("fields must be type str or list")

        if not isinstance(sym, list):
            raise Exception("sym must be type str or list")

        if uel_idx == True and set(self.__symSource__.values()) == {1}:
            print(
                "WARNING: uel_idx set to True, but data in GdxContainar cannot support uel_idx, ignoring..."
            )
            uel_idx = False

        df = {}
        for n in sym:
            if n in self.__symLoaded__:

                df[n] = {}
                df[n]["type"] = self.symType[n]
                df[n]["dimension"] = self.symDim[n]
                df[n]["domain"] = self.symDomain[n]
                df[n]["domain_info"] = self.symDomainInfoType[n]
                df[n]["alias_with"] = self.symAliasWith[n]
                df[n]["number_records"] = self.symNrRecs[n]
                df[n]["text"] = self.symText[n]
                df[n]["elements"] = pd.DataFrame.from_records(self.symElements[n])

                # drop attr columns if necessary
                if self.symType[n] in {"set", "singleton_set", "alias"}:
                    df[n]["elements"].drop(columns=self.__record_attr__, inplace=True)

                else:
                    df[n]["elements"].drop(
                        columns=self.__record_attr__ - set(fields), inplace=True
                    )

                if uel_idx == False:
                    if self.__symSource__[n] == 0:
                        for field in ["dim" + str(j) for j in range(self.symDim[n])]:
                            df[n]["elements"][field] = df[n]["elements"][field].map(
                                self.__strUels__
                            )
                            df[n]["elements"][field] = df[n]["elements"][field].map(str)

                # map in domain symbols to dataframe columns
                df[n]["elements"].rename(
                    columns={
                        "dim" + str(j): self.symDomain[n][j]
                        for j in range(self.symDim[n])
                    },
                    inplace=True,
                )

            else:
                print(
                    f'WARNING: symbol "{n}" not currently loaded into memory, ignoring.'
                )

        if len(sym) == 1:
            return df[sym[0]]
        else:
            return df

    def __infer_dimension__(self, data, symbol_name):
        if data[symbol_name]["type"] in {"set", "singleton_set"}:
            # Or(set, list, str, int, np.int64, tuple, np.recarray)
            if isinstance(data[symbol_name]["elements"], (str, int, np.int64)):
                return 1

            if isinstance(data[symbol_name]["elements"], tuple):
                return len(data[symbol_name]["elements"])

            if isinstance(data[symbol_name]["elements"], set):
                first_elem = list(data[symbol_name]["elements"])[0]
                if isinstance(first_elem, (str, int)):
                    return 1
                if isinstance(first_elem, tuple):
                    return len(first_elem)

            if isinstance(data[symbol_name]["elements"], list):
                if isinstance(data[symbol_name]["elements"][0], (str, int)):
                    return 1
                if isinstance(data[symbol_name]["elements"][0], tuple):
                    return len(data[symbol_name]["elements"][0])

            if isinstance(data[symbol_name]["elements"], np.recarray):
                return len(
                    set(data[symbol_name]["elements"].dtype.names)
                    - self.__record_attr__
                )

        elif data[symbol_name]["type"] in {"scalar"}:
            # Or(int, float, np.int64, np.float64, np.recarray)
            if isinstance(
                data[symbol_name]["elements"], (int, np.int64, np.float64, float)
            ):
                return 0

            if isinstance(data[symbol_name]["elements"], np.recarray):
                return len(
                    set(data[symbol_name]["elements"].dtype.names)
                    - self.__record_attr__
                )

        elif data[symbol_name]["type"] in {"alias"}:
            return -1

        elif data[symbol_name]["type"] in {"parameter", "equation", "variable"}:
            # Or(dict, np.recarray)
            if isinstance(data[symbol_name]["elements"], dict):
                firstkey = list(data[symbol_name]["elements"].keys())[0]
                if isinstance(firstkey, (str, int, np.int64)):
                    return 1
                if isinstance(firstkey, tuple):
                    return len(firstkey)

            # if user enters parameter data as a numpy record array
            if isinstance(data[symbol_name]["elements"], np.recarray):
                return len(
                    set(data[symbol_name]["elements"].dtype.names)
                    - self.__record_attr__
                )
        else:
            raise Exception(
                f'unknown symbol type detected for "{symbol_name}" when attempting to infer dimensionality'
            )

    def __set_dimension__(self, data, symbol_name):
        if "dimension" not in data[symbol_name].keys():
            data[symbol_name]["dimension"] = self.__infer_dimension__(data, symbol_name)

        else:
            infer_symdim = self.__infer_dimension__(data, symbol_name)
            if data[symbol_name]["dimension"] != infer_symdim:
                raise Exception(
                    f'Inconsistent data specification for "{symbol_name}": user dimension specified as {data[symbol_name]["dimension"]}, data inferred dimension of {infer_symdim}'
                )

    def __infer_nrecs__(self, data, symbol_name):
        if data[symbol_name]["type"] in {"set"}:
            # Or(list, tuple, str, np.recarray)
            if isinstance(data[symbol_name]["elements"], (str, tuple)):
                return 1

            if isinstance(data[symbol_name]["elements"], (list, np.recarray)):
                return len(data[symbol_name]["elements"])

        elif data[symbol_name]["type"] in {"scalar"}:
            # Or(int, float, np.recarray)
            if isinstance(data[symbol_name]["elements"], (int, float)):
                return 1

            if isinstance(data[symbol_name]["elements"], np.recarray):
                return len(data[symbol_name]["elements"])

        elif data[symbol_name]["type"] in {"singleton_set"}:
            return 1
            # if isinstance(data[symbol_name]["elements"], (str, tuple)):
            #     return 1
            #
            # if isinstance(data[symbol_name]["elements"], np.recarray):
            #     return len(data[symbol_name]["elements"])

        elif data[symbol_name]["type"] in {"alias"}:
            return -1

        elif data[symbol_name]["type"] in {"parameter", "equation", "variable"}:
            # Or(dict, np.recarray)
            if isinstance(data[symbol_name]["elements"], (dict, np.recarray)):
                return len(data[symbol_name]["elements"])
        else:
            raise Exception(
                f'unknown symbol type detected for "{symbol_name}" when attempting to infer number of records'
            )

    def __set_nrecs__(self, data, symbol_name):
        if "number_records" not in data[symbol_name].keys():
            data[symbol_name]["number_records"] = self.__infer_nrecs__(
                data, symbol_name
            )

        else:
            infer_nrecs = self.__infer_nrecs__(data, symbol_name)
            if data[symbol_name]["number_records"] != infer_nrecs:
                raise Exception(
                    f'Inconsistant data specification for "{symbol_name}": user number_records specified as {data[symbol_name]["number_records"]}, data inferred number_records of {infer_nrecs}'
                )

    def __infer_domain__(self, data, symbol_name):
        if data[symbol_name]["type"] in {
            "set",
            "singleton_set",
            "parameter",
            "variable",
            "equation",
        }:
            return ["*" for _ in range(self.__infer_dimension__(data, symbol_name))]

        elif data[symbol_name]["type"] in {"scalar"}:
            # Or(int, float, np.recarray)
            if isinstance(data[symbol_name]["elements"], (int, float, np.recarray)):
                return []

        elif data[symbol_name]["type"] in {"alias"}:
            return []

        else:
            raise Exception(
                f'unknown symbol type detected for "{symbol_name}" when attempting to infer domain'
            )

    def __set_domain__(self, data, symbol_name):
        if "domain" not in data[symbol_name].keys():
            data[symbol_name]["domain"] = self.__infer_domain__(data, symbol_name)

    def __infer_domain_info__(self, data, symbol_name):
        if not data[symbol_name]["domain"]:
            return "none"
        else:
            return "relaxed"

    def __set_domain_info__(self, data, symbol_name):
        if "domain_info" not in data[symbol_name].keys():
            data[symbol_name]["domain_info"] = self.__infer_domain_info__(
                data, symbol_name
            )

        else:
            infer_domain_info = self.__infer_domain_info__(data, symbol_name)
            if data[symbol_name]["domain_info"] != infer_domain_info:

                if data[symbol_name]["domain_info"] == "regular":
                    for i in set(data[symbol_name]["domain"]):
                        if i not in data.keys() and i not in [
                            s
                            for s, v in self.symType.items()
                            if v in {"set", "singleton_set"}
                        ]:
                            raise Exception(
                                f'User specified domain_info for "{symbol_name}" to be gdx type "regular" (domain check) but symbol contains a dimension set "{i}" that has not been specified in this data structure or already exists in GdxContainer (if "{i}" exists, perhaps its not a set)'
                            )
                else:
                    raise Exception(
                        f'Inconsistant data specification for "{symbol_name}": user domain_info specified as {data[symbol_name]["domain_info"]}, inferred domain_info of {infer_domain_info}'
                    )

    def __infer_alias_with__(self, data, symbol_name):
        if data[symbol_name]["type"] in {
            "scalar",
            "set",
            "singleton_set",
            "parameter",
            "variable",
            "equation",
        }:
            return None
        else:
            return data[symbol_name]["alias_with"]

    def __set_alias_with__(self, data, symbol_name):
        if "alias_with" not in data[symbol_name].keys():
            data[symbol_name]["alias_with"] = self.__infer_alias_with__(
                data, symbol_name
            )

        else:
            infer_alias_with = self.__infer_alias_with__(data, symbol_name)
            if data[symbol_name]["alias_with"] != infer_alias_with:
                raise Exception(
                    f'Inconsistant data specification for "{symbol_name}": user alias_with specified as {data[symbol_name]["alias_with"]}, inferred domain_info of {infer_alias_with}'
                )

    def __infer_text__(self, data, symbol_name):
        return ""

    def __set_text__(self, data, symbol_name):
        if "text" not in data[symbol_name].keys():
            data[symbol_name]["text"] = self.__infer_text__(data, symbol_name)

    def __convert_elements__(self, data, symbol_name, convert_vals=None):

        # set default
        if convert_vals == None:
            convert_vals = self.__map_to_numpy_float__

        # start converting
        if data[symbol_name]["type"] in {"alias"}:
            data[symbol_name]["elements"] = None

        else:
            if isinstance(data[symbol_name]["elements"], np.recarray):
                dts = [
                    (f"dim{i}", "object") for i in range(data[symbol_name]["dimension"])
                ]
                dts.extend(
                    [
                        ("L", "float"),
                        ("M", "float"),
                        ("LO", "float"),
                        ("UP", "float"),
                        ("SCALE", "float"),
                    ]
                )
                if data[symbol_name]["elements"].dtype != dts:
                    return Exception("user data does not exist in valid numpy format")

            if isinstance(data[symbol_name]["elements"], (int, np.int64)):
                values = [(float(data[symbol_name]["elements"]), 0.0, 0.0, 0.0, 0.0)]
                data[symbol_name]["elements"] = pd.DataFrame(
                    data=values, columns=self.__record_attr__
                )

                data[symbol_name]["elements"] = data[symbol_name][
                    "elements"
                ].to_records(index=False)

            if isinstance(data[symbol_name]["elements"], (float, np.float64)):
                values = [(data[symbol_name]["elements"], 0.0, 0.0, 0.0, 0.0)]
                data[symbol_name]["elements"] = pd.DataFrame(
                    data=values, columns=self.__record_attr__
                )

                # map to numpy floats
                idx = data[symbol_name]["elements"][
                    data[symbol_name]["elements"]["L"].isin(convert_vals.keys()) == True
                ].index
                data[symbol_name]["elements"].loc[idx, "L"] = (
                    data[symbol_name]["elements"].loc[idx, "L"].map(convert_vals)
                )

                # create recarray
                data[symbol_name]["elements"] = data[symbol_name][
                    "elements"
                ].to_records(index=False)

            if isinstance(data[symbol_name]["elements"], (str, tuple)):
                data[symbol_name]["elements"] = [data[symbol_name]["elements"]]
                data[symbol_name]["elements"] = pd.DataFrame(
                    data=data[symbol_name]["elements"],
                    columns=[
                        "dim" + str(j) for j in range(data[symbol_name]["dimension"])
                    ],
                    dtype="str",
                )
                data[symbol_name]["elements"]["L"] = 0.0
                data[symbol_name]["elements"]["M"] = 0.0
                data[symbol_name]["elements"]["LO"] = 0.0
                data[symbol_name]["elements"]["UP"] = 0.0
                data[symbol_name]["elements"]["SCALE"] = 0.0

                data[symbol_name]["elements"] = data[symbol_name][
                    "elements"
                ].to_records(index=False)

            if isinstance(data[symbol_name]["elements"], list):
                data[symbol_name]["elements"] = pd.DataFrame(
                    data=data[symbol_name]["elements"],
                    columns=[
                        "dim" + str(j) for j in range(data[symbol_name]["dimension"])
                    ],
                    dtype="str",
                )
                data[symbol_name]["elements"]["L"] = 0.0
                data[symbol_name]["elements"]["M"] = 0.0
                data[symbol_name]["elements"]["LO"] = 0.0
                data[symbol_name]["elements"]["UP"] = 0.0
                data[symbol_name]["elements"]["SCALE"] = 0.0

                data[symbol_name]["elements"] = data[symbol_name][
                    "elements"
                ].to_records(index=False)

            if data[symbol_name]["type"] in {"parameter"}:
                if isinstance(data[symbol_name]["elements"], dict):
                    elements = pd.DataFrame(
                        data=data[symbol_name]["elements"].keys(),
                        columns=[
                            "dim" + str(j)
                            for j in range(data[symbol_name]["dimension"])
                        ],
                        dtype="str",
                    )
                    elements["L"] = data[symbol_name]["elements"].values()
                    elements["M"] = 0.0
                    elements["LO"] = 0.0
                    elements["UP"] = 0.0
                    elements["SCALE"] = 0.0

                    # map to numpy floats
                    idx = elements[
                        elements["L"].isin(convert_vals.keys()) == True
                    ].index

                    if not idx.empty:
                        elements.loc[idx, "L"] = elements.loc[idx, "L"].map(
                            convert_vals
                        )
                        elements["L"] = elements["L"].map(float)

                    # create recarray
                    data[symbol_name]["elements"] = elements.to_records(index=False)

            if data[symbol_name]["type"] in {"variable", "equation"}:
                if isinstance(data[symbol_name]["elements"], dict):
                    elements = pd.DataFrame(
                        data=data[symbol_name]["elements"].keys(),
                        columns=[
                            "dim" + str(j)
                            for j in range(data[symbol_name]["dimension"])
                        ],
                        dtype="str",
                    )

                    vals = [
                        (
                            data[symbol_name]["elements"][e]["L"],
                            data[symbol_name]["elements"][e]["M"],
                            data[symbol_name]["elements"][e]["LO"],
                            data[symbol_name]["elements"][e]["UP"],
                            data[symbol_name]["elements"][e]["SCALE"],
                        )
                        for e in data[symbol_name]["elements"]
                    ]

                    elements["L"] = 0.0
                    elements["M"] = 0.0
                    elements["LO"] = 0.0
                    elements["UP"] = 0.0
                    elements["SCALE"] = 0.0
                    elements[["L", "M", "LO", "UP", "SCALE"]] = vals

                    # map to numpy floats
                    for field in self.__record_attr__:
                        idx = elements[
                            elements[field].isin(convert_vals.keys()) == True
                        ].index

                        if not idx.empty:
                            elements.loc[idx, field] = elements.loc[idx, field].map(
                                convert_vals
                            )
                        if elements[field].dtype != float:
                            elements[field] = elements[field].map(float)

                    data[symbol_name]["elements"] = elements.to_records(index=False)

    def __map_gdx_special_values__(self, symbol_name):

        elements = pd.DataFrame.from_records(self.symElements[symbol_name])

        if self.__symSource__[symbol_name] == 0:
            for field in ["dim" + str(j) for j in range(self.symDim[symbol_name])]:
                elements[field] = elements[field].map(self.__strUels__)
                elements[field] = elements[field].map(str)

        if self.symType[symbol_name] in {"parameter", "variable", "equation"}:
            # map to gdx special values
            for field in self.__record_attr__:
                idx = elements[
                    elements[field].isin(self.__gdxSpecialValuesWriteAs__.keys())
                    == True
                ].index

                if not idx.empty:
                    elements.loc[idx, field] = elements.loc[idx, field].map(
                        self.__gdxSpecialValuesWriteAs__
                    )
                if elements[field].dtype != float:
                    elements[field] = elements[field].map(float)

        return elements.to_records(index=False)

    def standardize(self, data, inplace=False, convert_vals=None):
        if not isinstance(inplace, bool):
            raise Exception("inplace must be type bool")

        # set default
        if convert_vals == None:
            convert_vals = self.__map_to_numpy_float__

        if inplace == False:
            data = copy.deepcopy(data)

            # standardize the data
            for i in data.keys():
                # pre-convert for set and singleton_set
                if data[i]["type"] in {"set", "singleton_set"}:
                    if isinstance(data[i]["elements"], (int, np.int64)):
                        data[i]["elements"] = str(data[i]["elements"])

                    if isinstance(data[i]["elements"], set):
                        data[i]["elements"] = list(data[i]["elements"])

                self.__set_dimension__(data, i)
                self.__set_nrecs__(data, i)
                self.__set_domain__(data, i)
                self.__set_domain_info__(data, i)
                self.__set_alias_with__(data, i)
                self.__set_text__(data, i)

                # convert the elements to standard numpy form
                begin = time.time()
                print(f'begin converting "{i}" to standard numpy format')
                self.__convert_elements__(data, i, convert_vals=convert_vals)
                dt = time.time() - begin
                print(f"... finished converting data... {round(dt,3)} sec")

            return data

        else:

            for i in data.keys():

                # pre-convert for set and singleton_set
                if data[i]["type"] in {"set", "singleton_set"}:
                    if isinstance(data[i]["elements"], (int, np.int64)):
                        data[i]["elements"] = [str(data[i]["elements"])]

                    if isinstance(data[i]["elements"], set):
                        data[i]["elements"] = list(data[i]["elements"])

                self.__set_dimension__(data, i)
                self.__set_nrecs__(data, i)
                self.__set_domain__(data, i)
                self.__set_domain_info__(data, i)
                self.__set_alias_with__(data, i)
                self.__set_text__(data, i)

                # convert the elements to standard numpy form
                begin = time.time()
                print(f'begin converting "{i}" to standard numpy format')
                self.__convert_elements__(data, i, convert_vals=convert_vals)
                dt = time.time() - begin
                print(f"... finished converting data... {round(dt,3)} sec")

    def __check_long_domains__(self, data, symbol_name):
        if data[symbol_name]["type"] in {
            "set",
            "parameter",
            "singleton_set",
            "variable",
            "equation",
        }:

            if not isinstance(data[symbol_name]["elements"], np.recarray):
                raise Exception(
                    "long domain name checking requires data to be in standard form"
                )

            for n in data[symbol_name]["elements"].tolist():
                for nn in list(n[0:-5]):
                    if len(nn) > GMS_UEL_IDENT_SIZE:
                        raise Exception(
                            f'long domain names detected in symbol "{symbol_name}"'
                        )

    def __check_absurd_numbers__(self, data, symbol_name):
        if data[symbol_name]["type"] in {"parameter", "variable", "equation"}:

            if not isinstance(data[symbol_name]["elements"], np.recarray):
                raise Exception(
                    "data size checking requires data to be in standard form"
                )

            huge_count = 0
            for n in data[symbol_name]["elements"].tolist():
                if n[-5] > 1e300:
                    huge_count += 1

            if huge_count != 0:
                raise Exception(
                    f'{huge_count} level value(s) (>1e300) detected in symbol "{symbol_name}", must fix'
                )

    def deep_validate(self, data):
        # check if data follows the schema
        for i in data.keys():
            if self.__deep_validate_schema__(data, i) == False:
                raise Exception(f'data for symbol "{i}" does NOT follow data schema')
        print("all data OK")

    def validate(self, data):
        # check if data follows the schema
        for i in data.keys():
            if self.__validate_schema__(data, i) == False:
                raise Exception(f'data for symbol "{i}" does NOT follow data schema')
        print("all data OK")

    def __validate_schema__(self, data, symbol_name):
        if "type" not in data[symbol_name].keys():
            raise Exception(
                f'data structure for symbol "{symbol_name}" must include "type" (set, scalar, parameter, singleton_set, variable, equation, alias)'
            )

        if "alias_with" not in data[symbol_name].keys() and data[symbol_name][
            "type"
        ] in {"alias"}:
            raise Exception(
                f'data structure for alias "{symbol_name}" must include "alias_with" information'
            )

        if "elements" not in data[symbol_name].keys() and data[symbol_name]["type"] in {
            "set",
            "scalar",
            "parameter",
            "singleton_set",
            "variable",
            "equation",
        }:
            raise Exception(
                f'data structure for symbol "{symbol_name}" must include "elements" information'
            )

        schema = {}
        schema["scalar"] = Schema(
            {
                "type": "scalar",
                "elements": Or(int, float, np.int64, np.float64, np.recarray),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): lambda n: not n,
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["parameter"] = Schema(
            {
                "type": "parameter",
                "elements": Or(dict, np.recarray),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["set"] = Schema(
            {
                "type": "set",
                "elements": Or(set, list, str, int, tuple, np.int64, np.recarray),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["singleton_set"] = Schema(
            {
                "type": "singleton_set",
                "elements": Or(
                    Schema((str, int, np.int64)), set, str, int, np.int64, np.recarray,
                ),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["alias"] = Schema(
            {
                "type": "alias",
                "alias_with": str,
                Optional("text"): str,
                Optional("dimension"): -1,
                Optional("number_records"): -1,
                Optional("domain"): [],
                Optional("domain_info"): "none",
                Optional("elements"): lambda n: n == None,
            }
        )

        schema["variable"] = Schema(
            {
                "type": "variable",
                "elements": Or(dict, np.recarray),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["equation"] = Schema(
            {
                "type": "equation",
                "elements": Or(dict, np.recarray),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        return schema[data[symbol_name]["type"]].is_valid(data[symbol_name])

    def __deep_validate_schema__(self, data, symbol_name):
        if "type" not in data[symbol_name].keys():
            raise Exception(
                f'data structure for symbol "{symbol_name}" must include "type" (set, scalar, parameter, singleton_set, variable, equation, alias)'
            )

        if "alias_with" not in data[symbol_name].keys() and data[symbol_name][
            "type"
        ] in {"alias"}:
            raise Exception(
                f'data structure for alias "{symbol_name}" must include "alias_with" information'
            )

        if "elements" not in data[symbol_name].keys() and data[symbol_name]["type"] in {
            "set",
            "scalar",
            "parameter",
            "singleton_set",
            "variable",
            "equation",
        }:
            raise Exception(
                f'data structure for symbol "{symbol_name}" must include "elements" information'
            )

        schema = {}
        schema["scalar"] = Schema(
            {
                "type": "scalar",
                "elements": Or(int, float, np.int64, np.float64, np.recarray),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): lambda n: not n,
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["parameter"] = Schema(
            {
                "type": "parameter",
                "elements": Or(
                    Schema(
                        {
                            Or(str, tuple, int, np.int64): Or(
                                int, float, np.int64, np.float64, str
                            )
                        }
                    ),
                    np.recarray,
                ),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["set"] = Schema(
            {
                "type": "set",
                "elements": Or(
                    Schema([str, tuple, int, np.int64]),
                    Schema((str, int, np.int64)),
                    set,
                    str,
                    int,
                    np.recarray,
                ),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["singleton_set"] = Schema(
            {
                "type": "singleton_set",
                "elements": Or(
                    Schema((str, int, np.int64)), set, str, int, np.int64, np.recarray,
                ),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["alias"] = Schema(
            {
                "type": "alias",
                "alias_with": str,
                Optional("text"): str,
                Optional("dimension"): -1,
                Optional("number_records"): -1,
                Optional("domain"): [],
                Optional("domain_info"): "none",
                Optional("elements"): lambda n: n == None,
            }
        )

        schema["variable"] = Schema(
            {
                "type": "variable",
                "elements": Or(
                    Schema(
                        {
                            Schema(str): Schema(
                                {
                                    "L": Or(int, float, np.int64, np.float64, str),
                                    "M": Or(int, float, np.int64, np.float64, str),
                                    "LO": Or(int, float, np.int64, np.float64, str),
                                    "UP": Or(int, float, np.int64, np.float64, str),
                                    "SCALE": Or(int, float, np.int64, np.float64, str),
                                }
                            )
                        }
                    ),
                    Schema(
                        {
                            Schema((int, np.int64, str)): Schema(
                                {
                                    "L": Or(int, float, np.int64, np.float64, str),
                                    "M": Or(int, float, np.int64, np.float64, str),
                                    "LO": Or(int, float, np.int64, np.float64, str),
                                    "UP": Or(int, float, np.int64, np.float64, str),
                                    "SCALE": Or(int, float, np.int64, np.float64, str),
                                }
                            )
                        }
                    ),
                    np.recarray,
                ),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        schema["equation"] = Schema(
            {
                "type": "equation",
                "elements": Or(
                    Schema(
                        {
                            Schema(str): Schema(
                                {
                                    "L": Or(int, float, np.int64, np.float64, str),
                                    "M": Or(int, float, np.int64, np.float64, str),
                                    "LO": Or(int, float, np.int64, np.float64, str),
                                    "UP": Or(int, float, np.int64, np.float64, str),
                                    "SCALE": Or(int, float, np.int64, np.float64, str),
                                }
                            )
                        }
                    ),
                    Schema(
                        {
                            Schema((int, np.int64, str)): Schema(
                                {
                                    "L": Or(int, float, np.int64, np.float64, str),
                                    "M": Or(int, float, np.int64, np.float64, str),
                                    "LO": Or(int, float, np.int64, np.float64, str),
                                    "UP": Or(int, float, np.int64, np.float64, str),
                                    "SCALE": Or(int, float, np.int64, np.float64, str),
                                }
                            )
                        }
                    ),
                    np.recarray,
                ),
                Optional("number_records"): int,
                Optional("dimension"): int,
                Optional("alias_with"): lambda n: n == None,
                Optional("text"): str,
                Optional("domain"): Schema([str]),
                Optional("domain_info"): lambda n: n in self.__domain_info_types__,
            }
        )

        return schema[data[symbol_name]["type"]].is_valid(data[symbol_name])

    def __standard_schema__(self, data, symbol_name):
        schema = {}
        schema["scalar"] = Schema(
            {
                "type": "scalar",
                "elements": Or(int, float, np.int64, np.float64, np.recarray),
                "number_records": int,
                "dimension": int,
                "alias_with": lambda n: n == None,
                "text": str,
                "domain": lambda n: not n,
                "domain_info": lambda n: n in self.__domain_info_types__,
            }
        )

        schema["parameter"] = Schema(
            {
                "type": "parameter",
                "elements": Or(dict, np.recarray),
                "number_records": int,
                "dimension": int,
                "alias_with": lambda n: n == None,
                "text": str,
                "domain": Schema([str]),
                "domain_info": lambda n: n in self.__domain_info_types__,
            }
        )

        schema["set"] = Schema(
            {
                "type": "set",
                "elements": Or(list, str, int, np.int64, np.recarray),
                "number_records": int,
                "dimension": int,
                "alias_with": lambda n: n == None,
                "text": str,
                "domain": Schema([str]),
                "domain_info": lambda n: n in self.__domain_info_types__,
            }
        )

        schema["singleton_set"] = Schema(
            {
                "type": "singleton_set",
                "elements": Or(str, np.recarray),
                "number_records": int,
                "dimension": int,
                "alias_with": lambda n: n == None,
                "text": str,
                "domain": Schema([str]),
                "domain_info": lambda n: n in self.__domain_info_types__,
            }
        )

        schema["alias"] = Schema(
            {
                "type": "alias",
                "alias_with": str,
                "text": str,
                "dimension": -1,
                "number_records": -1,
                "domain": [],
                "domain_info": "none",
                "elements": lambda n: n == None,
            }
        )

        schema["variable"] = Schema(
            {
                "type": "variable",
                "elements": Or(dict, np.recarray),
                "number_records": int,
                "dimension": int,
                "alias_with": lambda n: n == None,
                "text": str,
                "domain": Schema([str]),
                "domain_info": lambda n: n in self.__domain_info_types__,
            }
        )

        schema["equation"] = Schema(
            {
                "type": "equation",
                "elements": Or(dict, np.recarray),
                "number_records": int,
                "dimension": int,
                "alias_with": lambda n: n == None,
                "text": str,
                "domain": Schema([str]),
                "domain_info": lambda n: n in self.__domain_info_types__,
            }
        )

        return schema[data[symbol_name]["type"]].is_valid(data[symbol_name])

    def add_to_gdx(
        self,
        data,
        standardize_data=False,
        inplace=False,
        quality_checks=False,
        convert_vals=None,
    ):
        """
        The add_to_gdx() method is used import add other data to a GdxContainer which can then be exported to a GDX file for use directly in GAMS.

        Parameters
        ----------

        Returns
        -------
        None


        Examples
        --------

        """
        if not isinstance(standardize_data, bool):
            raise Exception("standardize_data must be type bool")

        if not isinstance(inplace, bool):
            raise Exception("inplace must be type bool")

        if not isinstance(data, dict):
            raise Exception("user supplied data must be type dict")

        if not isinstance(quality_checks, bool):
            raise Exception("quality_checks must be type bool")

        if convert_vals != None and not isinstance(convert_vals, dict):
            raise Exception("user specified convert_vals must be type dict")

        if convert_vals != None:
            for i in convert_vals.values():
                if not isinstance(i, float):
                    raise Exception(
                        "user specified convert_vals must all map to class float, (i.e., isinstance(val, float) must evaluate to True)"
                    )

        # set default
        if convert_vals == None:
            convert_vals = self.__map_to_numpy_float__

        # check for exact symbol matches already in GdxContainer
        for i in data.keys():
            if i in self.syms:
                print(
                    f'WARNING: symbol "{i}" already exists in GdxContainer... overwriting'
                )

        # check for casefolded symbol matches
        casefolded_symbols = {i.casefold() for i in self.syms}
        for i in data.keys():
            if i.casefold() in casefolded_symbols - set(self.syms):
                raise Exception(
                    f'WARNING: attempting to add "{i}" which matches an existing -- but casefolded -- symbol. GAMS is case insensitive, rename symbol to ensure uniqueness.'
                )

        # check if data has been standardized
        if standardize_data == True:
            if inplace == False:
                data = self.standardize(
                    data, inplace=inplace, convert_vals=convert_vals
                )
            else:
                self.standardize(data, inplace=inplace, convert_vals=convert_vals)
        else:
            # check if data is actually in standard format
            for i in data.keys():
                if self.__standard_schema__(data, i) == False:
                    raise Exception(
                        f"data for symbol {i} is not in the standard format"
                    )

        for i in data.keys():
            if quality_checks == True:
                begin = time.time()
                print(f"begin data quality checks")
                print(f'checking for long domains in "{i}"')
                self.__check_long_domains__(data, i)
                print(f'... finished checking for long domains in "{i}"')
                print(f'checking for absurd numbers in "{i}"')
                self.__check_absurd_numbers__(data, i)
                print(f'... finished checking for absurd numbers in "{i}"')
                print(
                    f"finished all data quality checks... {round(time.time()-begin,3)} sec"
                )

            # move standardized data into GdxContainer
            self.symType[i] = data[i]["type"]
            self.symDim[i] = data[i]["dimension"]
            self.symNrRecs[i] = data[i]["number_records"]
            self.symDomain[i] = data[i]["domain"]
            self.symText[i] = data[i]["text"]
            self.symAliasWith[i] = data[i]["alias_with"]
            self.symDomainInfoType[i] = data[i]["domain_info"]
            self.symElements[i] = data[i]["elements"]
            self.__symSource__[i] = 1

            if i not in self.syms:
                self.syms.append(i)

            if i not in self.__symLoaded__:
                self.__symLoaded__.append(i)

    def __find_write_order__(self, output_graph=False, n_shells=2):
        # find a proper gdx write order
        # 1. Topologically sorted list of sets (build from a DAG)
        # 2. singleton sets
        # 3. alias sets
        # 4. parameters
        # 5. scalars
        # 6. variables
        # 7. equation data (not algebra)
        gdx_write_order = []

        set_map = {}
        for i in self.__symLoaded__:
            if self.symType[i] == "set" and i != "*":
                set_map[i] = self.symDomain[i]

        G = nx.DiGraph(incoming_graph_data=set_map)
        sorted_sets = list(nx.topological_sort(G))

        if not (not sorted_sets):
            sorted_sets.pop()
            gdx_write_order.extend(list(reversed(sorted_sets)))

        # ******
        # output graph of set_map
        if output_graph == True:
            nodes_per_shell = len(gdx_write_order) // n_shells
            r = len(gdx_write_order) - nodes_per_shell * n_shells

            shells = ["*"]
            n = 0
            for _ in range(n_shells - 1):
                shells.append(gdx_write_order[n : n + nodes_per_shell])
                n = n + nodes_per_shell
            shells.append(gdx_write_order[n:])

            pos = nx.layout.shell_layout(G, shells)
            nodes = nx.draw_networkx_nodes(
                G,
                pos,
                node_color="y",
                node_shape="h",
                node_size=100,
                edgecolors="k",
                linewidths=0.8,
                alpha=0.75,
            )
            edges = nx.draw_networkx_edges(
                G, pos, arrowstyle="->", arrowsize=6, width=0.8, alpha=0.8
            )

            labels = {k: f"{v}" for k, v in zip(list(G.nodes), list(G.nodes))}
            nx.draw_networkx_labels(G, pos, labels, font_size=6, font_weight="normal")

            ax = plt.gca()
            ax.set_axis_off()
            plt.savefig("domain_graph.png", dpi=600, format="png")
        # ******

        [
            gdx_write_order.append(i)
            for i in self.__symLoaded__
            if i not in gdx_write_order and self.symType[i] == "alias"
        ]

        [
            gdx_write_order.append(i)
            for i in self.__symLoaded__
            if i not in gdx_write_order and self.symType[i] == "singleton_set"
        ]

        [
            gdx_write_order.append(i)
            for i in self.__symLoaded__
            if i not in gdx_write_order and self.symType[i] == "parameter"
        ]

        [
            gdx_write_order.append(i)
            for i in self.__symLoaded__
            if i not in gdx_write_order and self.symType[i] == "scalar"
        ]

        [
            gdx_write_order.append(i)
            for i in self.__symLoaded__
            if i not in gdx_write_order and self.symType[i] == "variable"
        ]

        [
            gdx_write_order.append(i)
            for i in self.__symLoaded__
            if i not in gdx_write_order and self.symType[i] == "equation"
        ]

        assert len(gdx_write_order) == len(self.__symLoaded__) - sum(
            [1 for i in self.__symLoaded__ if i == "*"]
        )

        return gdx_write_order

    def write_gdx(self, gdxout, compress=False, domain_graph=False, n_shells=2):
        """
        The write_gdx() method is used export data contained in the GdxContainer to a GDX file for use directly in GAMS.

        Parameters
        ----------
        sym : string or list, not required. Defaults to all loaded symbols if input is NoneType. If an element in sym is not in self.__symLoaded__ nothing happens, the ValueError is simply skipped and a warning is issued. When multiple symbols are included in sym as a list, to_dataframe() returns a dict of dataframes.

        Returns
        -------
        None


        Examples
        --------

        """
        if not isinstance(gdxout, str):
            raise Exception("gdxout filename must be type str, required")

        if not isinstance(compress, bool):
            raise Exception(
                "compress must be of type bool, optional, default no compression"
            )

        if not isinstance(domain_graph, bool):
            raise Exception(
                "domain_graph must be of type bool, optional, default no graph output"
            )

        if os.path.isabs(gdxout) == False:
            if gdxout[0] == "~":
                gdxout = os.path.expanduser(gdxout)
            else:
                gdxout = os.path.abspath(gdxout)

        print(f"begin write to GDX")
        print(f'writing GDX to "{gdxout}"')

        #
        #
        # write gdx
        gdxHandle = new_gdxHandle_tp()
        rc = gdxCreateD(gdxHandle, self.gams_sysdir, GMS_SSSIZE)
        assert rc[0], rc[1]
        print(f"writing gdx using GDX DLL version: {gdxGetDLLVersion(gdxHandle)[1]}")

        # set GDXCOMPRESS environment variable for GDX compression
        if compress == False:
            print("no gdx compression")
            os.environ["GDXCOMPRESS"] = "0"
        if compress == True:
            print("writing gdx with compression")
            os.environ["GDXCOMPRESS"] = "1"

        assert gdxOpenWrite(gdxHandle, gdxout, "")[0]

        # register any set text
        if not self.__setText__:
            print("no gdx set text to register")
        else:
            [
                gdxAddSetText(gdxHandle, self.__setText__[k])
                for k in self.__setText__.keys()
            ]

        # find proper symbol write order to enable domain checking
        gdx_write_order = self.__find_write_order__(
            output_graph=domain_graph, n_shells=n_shells
        )
        print(f"gdx symbols will be written in the following order: {gdx_write_order}")

        #
        #
        # start writing
        errors = {}
        for i in gdx_write_order:
            print('writing symbol "{}" as type "{}"'.format(i, self.symType[i]))

            if self.symType[i] == "singleton_set":
                subtype = 1.0
            else:
                subtype = 0.0

            # write alias symbols
            if self.symType[i] == "alias":
                gdxAddAlias(gdxHandle, self.symAliasWith[i], i)
            else:
                type, subtype, dimen = self.__py2gdxtypemap__[self.symType[i]]

                assert gdxDataWriteStrStart(
                    gdxHandle, i, self.symText[i], self.symDim[i], type, subtype
                )

            # set up for domain checking if 'domain' was specified, no domain_info for alias
            if self.symDomainInfoType[i] in {"regular"} and self.symType[i] != "alias":

                for j in self.symDomain[i]:
                    if j not in self.__symLoaded__:
                        raise Exception(
                            f'symbol "{i}" contains a domain set "{j}" that has not been loaded, load all necessary domain sets when domain checking'
                        )

                assert gdxSymbolSetDomain(gdxHandle, self.symDomain[i])

            elif self.symDomainInfoType[i] in {"relaxed"}:
                print(f'"{i}" not being domain checked (writing in relaxed mode)')

            if self.symType[i] == "alias":
                print(f'"{i}" is an alias and does not require domain checking')

            # write all other symbols (not alias)
            if self.symType[i] in {
                "set",
                "singleton_set",
                "parameter",
                "scalar",
                "variable",
                "equation",
            }:

                dim_names = list(self.symElements[i].dtype.names[0 : self.symDim[i]])

                values = doubleArray(GMS_VAL_MAX)

                with tqdm(total=self.symNrRecs[i], file=sys.stdout) as pbar:
                    for n in self.__map_gdx_special_values__(i).tolist():
                        domain = list(n[0:-5])

                        values[GMS_VAL_LEVEL] = n[-5]
                        values[GMS_VAL_MARGINAL] = n[-4]
                        values[GMS_VAL_LOWER] = n[-3]
                        values[GMS_VAL_UPPER] = n[-2]
                        values[GMS_VAL_SCALE] = n[-1]

                        assert gdxDataWriteStr(gdxHandle, domain, values)
                        pbar.update(1)

                    assert gdxDataWriteDone(gdxHandle)

                errors[i] = gdxDataErrorCount(gdxHandle)

        # report errors
        if sum(errors.values()) != 0:
            for i in errors.keys():
                if errors[i] != 0:
                    print(f"{errors[i]} domain error(s) in {i}")
            gdxAutoConvert(gdxHandle, 1)
            assert not gdxClose(gdxHandle)
            assert gdxFree(gdxHandle)
            print("GDX not written successfully")

            if os.path.exists(gdxout):
                os.remove(gdxout)
        else:
            gdxAutoConvert(gdxHandle, 1)
            assert not gdxClose(gdxHandle)
            assert gdxFree(gdxHandle)

        print("All done")
