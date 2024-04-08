import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # Adding an identity column
    df['allotment_id'] = df.reset_index().index + 1
    # Adding an identity column
    df['contractor_id'] = df.reset_index().index + 1
    # Adding an identity column
    df['incharge_id'] = df.reset_index().index + 1
    # Adding an identity column
    df['personaldata_id'] = df.reset_index().index + 1
    dim_allotment = df[['Village','AllottedBlock','Plot_No']]

    # Adding an identity column
    dim_allotment['allotment_id'] = dim_allotment.reset_index().index + 1
    dim_contract = df[['NPI_ID','Contract_name','ICICI_PhysicalNumber','ICICI_OnlineNumber'
                   ,'ICICI_NotOpenedReason','ICICIBankAccount_Status']]
    # Adding an identity column
    dim_contract['contractor_id'] = dim_contract.reset_index().index + 1
    dim_incharge = df[['NPI_ID','LayoutIncharge_WAS','Housing_AE','Housing_DE']]
    # Adding an identity column
    dim_incharge['incharge_id'] = dim_incharge.reset_index().index + 1
    dim_pmay = df[['Survey_Code','Annexure','Pmay_ID','DPR','FaceAuthentication_Status']]
    dim_applicant = df[['ApplicantName','RelationName','Ward_No','CTR_No']]
    dim_personal = df[['NPI_ID','Mobile_Number','Phone_Number','Remarks','Land_Issues','Address']]
    # Adding an identity column
    dim_personal['personaldata_id'] = dim_personal.reset_index().index + 1

    fact_table = df.merge(dim_allotment, on='allotment_id') \
               .merge(dim_contract, on='contractor_id') \
               .merge(dim_incharge, on='incharge_id') \
               .merge(dim_pmay, on='Survey_Code') \
               .merge(dim_applicant, on='CTR_No') \
               .merge(dim_personal, on='personaldata_id') \
                [['Sl_No','NPI_ID_x','Ward_No_x', 'CTR_No','DPR_x','OnlineStage','Survey_Code',
                  'Pmay_ID_x','Annexure_x','ApplicantName_x','RelationName_x','Village_x','AllottedBlock_x',
                  'Plot_No_x','Contract_name_x','ICICI_PhysicalNumber_x','ICICI_OnlineNumber_x','ICICI_NotOpenedReason_x',
                  'ICICIBankAccount_Status_x','FaceAuthentication_Status_x','Mobile_Number_x','Phone_Number_x',
                  'Land_Issues_x','Remarks_x','LayoutIncharge_WAS_x','Housing_AE_x','Housing_DE_x','Address_x'] ]
    
    
    return {"dim_allotment":dim_allotment.to_dict(orient="dict"),
    "dim_contract":dim_contract.to_dict(orient="dict"),
    "dim_incharge":dim_incharge.to_dict(orient="dict"),
    "dim_pmay":dim_pmay.to_dict(orient="dict"),
    "dim_applicant":dim_applicant.to_dict(orient="dict"),
    "dim_personal":dim_personal.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'