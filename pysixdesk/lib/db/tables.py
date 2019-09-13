from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
# from sqlalchemy import DateTime  # maybe it is worth using DateTime for mtime columns
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import LargeBinary
from sqlalchemy import ForeignKey
# from sqlalchemy import Boolean
# from sqlalchemy import JSON  # Json columns instead of compressed ini files ?

from sqlalchemy.orm import relationship

# Very helpful doc pages:
# https://docs.sqlalchemy.org/en/13/core/type_basics.html
# https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html

Base = declarative_base()


class DynamicCols:
    '''
    This class adds a class method which would allow for dynamic setting of
    columns.
    '''

    @classmethod
    def add_cols(cls, names, col_type=Float):
        """Adds class attributes, i.e. columns to class.

        Args:
            names (list): List of strings, i.e. columns names.
            col_type (sqlalchemy type or list, optional): Either a sqlalchemy
            type, or a list containing sqlalchemy types of same length than
            "names".

        Returns:
            mapped class: modified mapped class.

        Raises:
            ValueError: If "col_type" if list and there is a length mismatch.
        """
        if isinstance(col_type, list):
            if len(names) != len(col_type):
                raise ValueError('"names" and "col_type" len mismatch.')
            for n, t in zip(names, col_type):
                setattr(cls, n, Column(t))
        else:
            for n in names:
                setattr(cls, n, Column(col_type))
        return cls


# PREPROCESSING


class PreprocessWU(Base, DynamicCols):

    __tablename__ = 'preprocess_wu'

    id = Column(Integer, primary_key=True)
    job_name = Column(String)
    batch_name = Column(String)
    unique_id = Column(Integer)
    status = Column(String)
    task_id = Column(Integer)
    mtime = Column(BigInteger)

    preprocess_task = relationship('PreprocessTask', backref='preprocess_wu')
    oneturn_sixtrack_result = relationship('OneturnSixtrackResult',
                                           backref='preprocess_wu')
    sixtrack_wu = relationship('SixtrackWU', backref='preprocess_wu')


class PreprocessTask(Base):

    __tablename__ = 'preprocess_task'

    id = Column(Integer, primary_key=True)
    wu_id = Column(Integer, ForeignKey('preprocess_wu.id'))
    madx_in = Column(LargeBinary)
    madx_stdout = Column(LargeBinary)
    job_stdout = Column(LargeBinary)
    job_stder = Column(LargeBinary)
    job_stdlog = Column(LargeBinary)
    status = Column(String)
    mtime = Column(BigInteger)
    fort_2 = Column(LargeBinary)
    fort_3_mad = Column(LargeBinary)
    fort_3_aux = Column(LargeBinary)
    fort_3_aper = Column(LargeBinary)
    fort_8 = Column(LargeBinary)
    fort_16 = Column(LargeBinary)
    fort_34 = Column(LargeBinary)

    oneturn_sixtrack_result = relationship('OneturnSixtrackResult',
                                           backref='preprocess_task')


class OneturnSixtrackResult(Base):

    __tablename__ = 'oneturn_sixtrack_result'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('preprocess_task.id'))
    wu_id = Column(Integer, ForeignKey('preprocess_wu.id'))
    betax = Column(Float)
    betax2 = Column(Float)
    betay = Column(Float)
    betay2 = Column(Float)
    tunex = Column(Float)
    tuney = Column(Float)
    chromx = Column(Float)
    chromy = Column(Float)
    x = Column(Float)
    xp = Column(Float)
    y = Column(Float)
    yp = Column(Float)
    z = Column(Float)
    zp = Column(Float)
    chromx_s = Column(Float)
    chromy_s = Column(Float)
    chrom_eps = Column(Float)
    tunex1 = Column(Float)
    tuney1 = Column(Float)
    tunex2 = Column(Float)
    tuney2 = Column(Float)
    mtime = Column(BigInteger)

# SIXTRACK


class SixtrackWU(Base, DynamicCols):
    __tablename__ = 'sixtrack_wu'

    id = Column(Integer, primary_key=True)
    preprocess_wu_id = Column(Integer, ForeignKey('preprocess_wu.id'))
    job_name = Column(String)
    input_file = Column(LargeBinary)
    batch_name = Column(String)
    unique_id = Column(String)
    status = Column(String)
    task_id = Column(Integer)
    boinc = Column(String)
    mtime = Column(BigInteger)

    sixtrack_task = relationship('SixtrackTask', backref='sixtrack_wu')
    sixtrack_result = relationship('SixtrackResult', backref='sixtrack_wu')


class SixtrackTask(Base):

    __tablename__ = 'sixtrack_task'

    id = Column(Integer, primary_key=True)
    wu_id = Column(Integer, ForeignKey('sixtrack_wu.id'))
    fort_3 = Column(LargeBinary)
    job_stdout = Column(LargeBinary)
    job_stder = Column(LargeBinary)
    job_stdlog = Column(LargeBinary)
    status = Column(LargeBinary)
    mtime = Column(BigInteger)
    fort_10 = Column(LargeBinary)

    sixtrack_result = relationship('SixtrackResult', backref='sixtrack_task')


class SixtrackResult(Base):

    __tablename__ = 'sixtrack_result'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('sixtrack_task.id'))
    wu_id = Column(Integer, ForeignKey('sixtrack_wu.id'))

    turn_max = Column(Integer)
    sflag = Column(Integer)

    emitx = Column(Float)
    emity = Column(Float)

    qx = Column(Float)
    qx_det = Column(Float)
    qx_spread = Column(Float)
    qy = Column(Float)
    qy_det = Column(Float)
    qy_spread = Column(Float)
    qs = Column(Float)

    xp = Column(Float)
    xy = Column(Float)
    qpx = Column(Float)
    qpy = Column(Float)

    deltap = Column(Float)

    betax = Column(Float)
    betay = Column(Float)
    betax2 = Column(Float)
    betay2 = Column(Float)

    sigx1 = Column(Float)
    sigy1 = Column(Float)
    sigx2 = Column(Float)
    sigy2 = Column(Float)

    smearx = Column(Float)
    smeary = Column(Float)
    smeart = Column(Float)

    dist = Column(Float)
    distp = Column(Float)

    resxact = Column(Float)
    resyact = Column(Float)
    resorder = Column(Float)

    sturns1 = Column(Float)
    sturns2 = Column(Float)

    sseed = Column(Float)

    sigxmin = Column(Float)
    sigxmax = Column(Float)
    sigxavg = Column(Float)
    sigymin = Column(Float)
    sigymax = Column(Float)
    sigyavg = Column(Float)
    sigxminld = Column(Float)
    sigxmaxld = Column(Float)
    sigxavgld = Column(Float)
    sigyminld = Column(Float)
    sigymaxld = Column(Float)
    sigyavgld = Column(Float)
    sigxminnld = Column(Float)
    sigxmaxnld = Column(Float)
    sigxavgnld = Column(Float)
    sigyminnld = Column(Float)
    sigymaxnld = Column(Float)
    sigyavgnld = Column(Float)

    delta = Column(Float)
    dnms = Column(Float)

    trttime = Column(Float)
    version = Column(Float)
    mtime = Column(BigInteger)

# OTHER


class BoincVars(Base):

    __tablename__ = 'boinc_vars'

    id = Column(Integer, primary_key=True)
    wu_name = Column(String)
    fpops_estimate = Column(Float)
    fpops_bound = Column(Float)
    mem_bound = Column(Integer)
    disk_bound = Column(Integer)
    delay_bound = Column(Integer)
    redundance = Column(Integer)
    copies = Column(Integer)
    errors = Column(Integer)
    issues = Column(Integer)
    results_witout_concensus = Column(Integer)
    app_name = Column(String)
    app_ver = Column(Integer)


class Env(Base):

    __tablename__ = 'env'

    id = Column(Integer, primary_key=True)
    madx_exe = Column(String)
    sixtrack_exe = Column(String)
    study_path = Column(String)
    preprocess_in = Column(String)
    preprocess_out = Column(String)
    sixtrack_in = Column(String)
    sixtrack_out = Column(String)
    gather = Column(String)
    templates = Column(String)
    boinc_spool = Column(String)
    test_turn = Column(Integer)
    bonc_work = Column(String)
    boinc_results = Column(String)
    surv_percent = Column(Integer)


class Templates(Base):

    __tablename__ = 'templates'

    id = Column(Integer, primary_key=True)
    mask = Column(LargeBinary)
    fort_3 = Column(LargeBinary)
