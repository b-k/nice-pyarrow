import pyarrow as pa
import pyarrow.compute as pc
from typing import Union, Dict, List, Optional, Callable
import re

_length = lambda x: len(x)

#Aggregations add '_sum' or '_avg' &c to column names, and it's work for users to track
#how to refer to their data. Remove the aggregation appendage.
remove_agg = lambda s: '_'.join(s.split('_')[0:-1]) if '_' in s else s

def maybe_subelmt(indata) -> Union[pa.ChunkedArray, float, int, list]:
    return (indata.vec if isinstance(indata, NiceVec)
            else pa.scalar(indata) if isinstance(indata, float) or isinstance(indata, int)
            else indata)

class NiceVec:
    def __init__(self, chunked_array_in: pa.ChunkedArray) -> None:
        self.vec = chunked_array_in
    
    def __add__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.add(self.vec, maybe_subelmt(other)))
    
    def __sub__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.subtract(self.vec, maybe_subelmt(other)))
    
    def __mul__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.multiply(self.vec,  maybe_subelmt(other)))
    
    def __truediv__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.divide(self.vec, maybe_subelmt(other)))

    def __radd__(self, other): return self + other
    def __rmul__(self, other): return self * other
    def __rsub__(self, other: Union['NiceVec', float, int]):
        return NiceVec(pc.subtract(maybe_subelmt(other), self.vec))

    def __rtruediv__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.divide(maybe_subelmt(other), self.vec))
    
    def __gt__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.greater(self.vec, maybe_subelmt(other)))

    def __ge__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.greater_equal(self.vec, maybe_subelmt(other)))
    
    def __lt__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.less(self.vec, maybe_subelmt(other)))

    def __le__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.less_equal(self.vec, maybe_subelmt(other)))

    def __eq__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.equal(self.vec, maybe_subelmt(other)))

    def __ne__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.not_equal(self.vec, maybe_subelmt(other)))

    def __and__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.and_(self.vec, maybe_subelmt(other)))

    def __or__(self, other: Union['NiceVec', float, int]) -> 'NiceVec':
        return NiceVec(pc.or_(self.vec, maybe_subelmt(other)))

    def __len__(self):  return len(self.vec)
    def __iter__(self): return iter(self.vec)

    def __getitem__(self, field: str):
        try:
            return self.vec[field].as_py()
        except AttributeError:
            return self.vec[field]

    def sum(self): return pc.sum(self.vec).as_py()

    def replace_if(self, condition:'NiceVec', other: Union['NiceVec', float, int]) -> 'NiceVec':
        """ Slightly different thinking from pc.if_else, and so different argument order. Here, keep what you have, but
         Replace it with the alternative if the condition is true."""
        return NiceVec(pc.if_else(condition.vec, maybe_subelmt(other), self.vec))

    def clip(self, lower: Union[None, 'NiceVec', float, int]=None,
                   upper: Union[None, 'NiceVec', float, int]=None) -> 'NiceVec':
        """If lower is set, replace any value in the vector less than lower with lower; similiarly
        with upper. Either limit may be a vector of the same length as the vector being clipped, in
        which case comparison is element-wise.
        If both are null, clip below at zero (i.e., replace all negative values with zero).
        """
        clipped_array = self.vec
        if lower is None and upper is None:
            lower=0
        if lower is not None:
            clipped_array = pc.max_element_wise(clipped_array, maybe_subelmt(lower))
        if upper is not None:
            clipped_array = pc.min_element_wise(clipped_array, maybe_subelmt(upper))
        return NiceVec(clipped_array)


class NiceTab:
    """Analogous to pyarrow.Table being a list of named pyarrow ChunkedArrays, this is a list of
    named NiceVecs.
    """
    def __init__(self, indata: pa.Table):
        self.tab = pa.Table.from_arrays(indata.columns, names=indata.column_names)

    def __len__(self) -> int: return len(self.tab)

    def _as_vec(self, indata) -> Union[pa.ChunkedArray, float, int, list]:
        # Let users give a plain scalar, string, [list of numbers], NiceVec, vector.
        # This returns whatever arbitrary format pyarrow wants.
        # Even this glitches---replacing a vector of strings requires a different
        # form from adding a vector of strings ([['a', 'b']] vs ['a', 'b']).
        return (indata.vec if isinstance(indata, NiceVec)
            else pa.chunked_array([[indata] * len(self)]) if isinstance(indata, float) or isinstance(indata, int)
            else [indata] * len(self) if  isinstance(indata, str)
            else pa.chunked_array([indata]) if isinstance(indata, list)
                                    and (isinstance(indata[0], float) or isinstance(indata[0], int))
            else indata)

    def filter(self, where=Union[pa.ChunkedArray, Callable[["NiceTab"], "NiceVec"], NiceVec, list]) -> "NiceTab":
        """Get a tab with only rows meeting the condition.
           Takes a vector (pyarrow, NiceVec, or simple list), or a lambda, which is called
           with this data set and should return a NiceVec.
        """
        if callable(where): where = where(self)
        return NiceTab(self.tab.filter(where.vec if isinstance(where, NiceVec) else where))

    def select(self, fields: Optional[Union[str, List[str]]]) -> "NiceTab":
        """Get a tab with only the columns named in your list."""
        fields = self.tab.column_names if not fields else [fields] if isinstance(fields, str) else fields
        return NiceTab(pa.Table.from_arrays([self[i].vec for i in fields], names=fields))

    def get(self, field: str) -> NiceVec:
        """Return a NiceVec with the column you requested.
           If not present, returns None, which facilitates `if not data.col` constructions"""
        try:
            return NiceVec(self.tab.column(field))
        except KeyError:
            return None

    def zeros(self) ->NiceVec:
        """Return a NiceVec of the same length as your table, filled with zeros.
           If you need a column of ones, try `ones_column = data.zeros()+1`.
        """
        return NiceVec(pa.chunked_array([[0]*len(self.tab)]))

    def __getitem__(self, field: str) -> NiceVec:  #Enables the bracketed stack[field] form on the RHS.
        return self.get(field)

    def __setitem__(self, key, val) -> 'NiceTab':  #Enables the bracketed stack[field] form on the LHS.
        return self.set({key: val})

    def __getattr__(self, field: str) -> 'NiceTab':
        return self.get(field)

    def Σ(self, *fields) -> 'NiceTab':
        """Add a list of fields together. E.g., your_tab.Σ("col1", "col2")."""
        return self.get(fields[0]).vec if len(fields)==1 \
               else pc.add(self.get(fields[0]).vec, self.Σ(*fields[1:]))

    def _check_for_dup_cols(self, addme: Dict[str, pa.ChunkedArray]):
        names = [n for n in addme] #The dict may be modified, so get all names first.
        for name in names:
            posn = [i for i,k in enumerate(self.tab.column_names) if k==name]
            if len(posn)>0:
                self.tab = self.tab.set_column(posn[0], name, self._as_vec(addme[name]))
                del addme[name]

    def set(self, addme: Dict[str, pa.ChunkedArray]) -> "NiceTab":
        self._check_for_dup_cols(addme) #Addme may be modified.
        self.tab = pa.Table.from_arrays(
            self.tab.columns + [self._as_vec(addme[k]) for k in addme],
            names= self.tab.column_names + list(addme.keys())
            )
        return self

    def __setattr__(self, field, val) -> "NiceTab":
        if field[0]!='_' and field!='tab' and field not in self.__dict__ and not callable(val):
            self.set({field:val})
        else:
            super().__setattr__(field, val)
        return self

    def __repr__(self): return self.tab.__repr__()
    def __str__(self): return self.tab.__str__()

    def _build_weighted_tab(self, what: List[str], groupings: List[str], weight: str, normalize: bool):
        total_weight = self[weight].sum()+0.0 if normalize else 1
        return pa.Table.from_arrays(
              [self.get(g).vec for g in groupings] + [(self[weight] * self.get(col)/total_weight).vec for col in what],
              names = groupings + what)

    def _apply_aggregation(self, intab, select, group_by, statistic, weight, normalize):
        if len(group_by)>0 and statistic=="count" and weight:
            total_weight = intab[weight].sum() if normalize else 1
            return intab.group_by(group_by).aggregate([(weight, stat)]).sort_by(group_by[0])/total_weight

        if len(group_by)>0:
            grouped = intab.group_by(group_by)
            stat= ("sum" if statistic is None or statistic=="sum" else
                   "mean" if statistic=="mean" or statistic=="avg" or statistic=="average" else
                   "min" if statistic=="min" else
                   "max" if statistic=="max" else
                   "count" if statistic=="count" else "error")
            return grouped.aggregate([(s, stat) for s in select]).sort_by(group_by[0])
        stat= (pc.sum if statistic is None or statistic=="sum" else
               pc.mean if statistic=="mean" or statistic=="avg" or statistic=="average" else
               pc.min if statistic=="min" else
               pc.max if statistic=="max" else
               _length if statistic=="count" and not weight else
               lambda _: intab[weight].sum() if statistic=="count"
               else "error")
        return pa.Table.from_arrays([pa.array([stat(intab[c])]) for c in select], names=select)

    def Q(self, select: Union[str, List[str]]=[],
                where: Union['NiceTab', Callable] = [],
                aggregation: Optional[str] = None,
                group_by: Union[str, List[str]] = [],
                weight: Optional[str] = None,
                weight_normalize: bool = True,
                df_out = True,
                html_out = None,
                append = 'w'
                ):
        """See the output section in Readme.md for details."""

        as_list = lambda s: [s] if isinstance(s, str) else s   #Bare "s" ⇒ list ["s"] if needed.
        if len(select)==0:
            select = list(set(self.tab.column_names).difference(set(as_list(group_by))))
        (select, group_by) = (as_list(select), as_list(group_by))
        if callable(where): where = where(self) #Apply a λ.

        out = (self if select is None else self.select(select + group_by + ([weight] if weight else []))).tab

        if len(where)>0: out = out.filter(where.vec)

        if len(group_by) > 0 or aggregation is not None:
            if weight: out = NiceTab(out)._build_weighted_tab(select, group_by, weight, weight_normalize) 
            out = self._apply_aggregation(out, select, group_by, aggregation, weight, weight_normalize)
            out = out.rename_columns([remove_agg(c) for c in out.column_names])
        
        if html_out is not None:
            h_out = open(f"out/{html_out}.html", append)
            h_out.write(out.to_pandas().T.to_html())
        return (out.to_pandas().set_index(group_by) if group_by else out.to_pandas()) if df_out else out
