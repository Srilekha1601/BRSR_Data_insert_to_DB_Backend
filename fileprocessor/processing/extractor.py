import pandas as pd
import os
import json
import re
import xml.etree.ElementTree as ET
from .xml_utils import transform_xml_to_array_of_JSON
from .condition_utils import conditon_on_df
def section_wise_data_extraction_from_xml_by_template(template_file, xml_file, output_dir):
    # Read sheet names from the uploaded Excel file
    sheet_names = pd.ExcelFile(template_file).sheet_names
    sheet_dfs = {}
    missing_dfs = {}

    # Parse XML and extract data
    output_strings = transform_xml_to_array_of_JSON(xml_file)
    df_result = pd.DataFrame(output_strings)

    # Extract filename for output
    filename_df = df_result[df_result['tag_name'] == "NameOfTheCompany"]
    filename = filename_df["text_value"].iloc[0]
    output_filename = f"{filename}_extracted_data_17_02_25_AMB2.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    extracted_df_renamed = df_result.rename(
        columns={"tag_name": "XML Tag", "context_ref": "Context Reference", "text_value": "Extracted Value"}
    )

    all_template_tags = pd.DataFrame(columns=["XML Tag", "Context Reference", "Sheet Name", "Xform Flag", "XFrom Transformation"])
    for sheet_name in sheet_names:
        template_df = pd.read_excel(template_file, sheet_name=sheet_name)
        template_df["Sheet Name"] = sheet_name
        all_template_tags = pd.concat([all_template_tags, template_df[["XML Tag", "Context Reference", "Sheet Name", "Xform Flag", "XFrom Transformation"]]])

    all_template_tags = all_template_tags.drop_duplicates()

    unique_xml_tags_sheet_names = (
        all_template_tags.groupby("XML Tag").agg({
            "Sheet Name": lambda x: ", ".join(sorted(x.unique())),
            **{col: "first" for col in all_template_tags.columns if col not in ["XML Tag", "Sheet Name"]},
        }).reset_index().rename(columns={"Sheet Name": "Associated Sheet Names"})
    )

    extracted_df_renamed['XML Tag_1'] = extracted_df_renamed['XML Tag'].str.lower()
    all_template_tags['XML Tag_1'] = all_template_tags['XML Tag'].str.lower()

    extra_tags = extracted_df_renamed.merge(all_template_tags, on=["XML Tag_1"], how="left", indicator=True)
    extracted_df_renamed.drop(columns=["XML Tag_1"], inplace=True)
    extra_tags = extra_tags[extra_tags["_merge"] == "left_only"].drop(columns=["_merge"])
    extra_tags.drop(columns=["XML Tag_1", "XML Tag_y", "Context Reference_y", "Xform Flag", "XFrom Transformation"], inplace=True)

    extra_tags_only = extracted_df_renamed.merge(all_template_tags, on=["Context Reference"], how="left", indicator=True)
    extra_tags_only = extra_tags_only[extra_tags_only["_merge"] == "left_only"].drop(columns=["_merge"])
    extra_tags_only = extra_tags_only.drop(columns=["XML Tag_y", "Sheet Name", "XFrom Transformation", "XML Tag_1", "Xform Flag"])
    extra_tags_only_renamed = extra_tags_only.rename(columns={"XML Tag_x": "XML Tag"})

    merged_df_1 = extra_tags_only_renamed.merge(
        unique_xml_tags_sheet_names,
        on="XML Tag",
        how="left"
    )
    merged_df_1.rename(columns={"Context Reference_x": "Context Reference"}, inplace=True)
    merged_df_1.drop(columns=["Context Reference_y"], inplace=True)

    for sheet_name in sheet_names:
        template_df = pd.read_excel(template_file, sheet_name=sheet_name)

        template_df["XML Tag_T"] = template_df["XML Tag"]
        template_df["Context Referenc_T"] = template_df["Context Reference"]

        template_df['XML Tag'] = template_df['XML Tag'].str.lower()
        template_df['Context Reference'] = template_df['Context Reference'].str.lower()

        extracted_df_renamed['XML Tag'] = extracted_df_renamed['XML Tag'].str.lower()
        extracted_df_renamed['Context Reference'] = extracted_df_renamed['Context Reference'].str.lower()

        merged_df = pd.merge(
            template_df,
            extracted_df_renamed,
            on=["XML Tag", "Context Reference"],
            how="left"
        )

        merged_df["XML Tag"] = template_df["XML Tag_T"]
        merged_df["Context Reference"] = template_df["Context Referenc_T"]
        merged_df.drop(columns=["XML Tag_T", "Context Referenc_T"], inplace=True)

        missing_df = merged_df[merged_df['Extracted Value'].isna()]
        if not missing_df.empty:
            missing_dfs[f"{sheet_name}_missing_in_xml"] = missing_df[['XML Tag', 'Context Reference']]

        # Call your condition logic here
        merged_df_after_condition = conditon_on_df(merged_df)
        sheet_dfs[sheet_name] = merged_df_after_condition

        for _, row in merged_df_1.iterrows():
            xml_tag = row["XML Tag"]
            context_ref = row["Context Reference"]
            extracted_value = row["Extracted Value"]
            associated_sheets = row["Associated Sheet Names"]
            Xform_Flag = row["Xform Flag"]
            XFrom_Transformation = row["XFrom Transformation"]

            if pd.notna(associated_sheets):
                sheet_list = [sheet.strip() for sheet in associated_sheets.split(",")]

                for sheet_name in sheet_list:
                    if sheet_name in sheet_dfs:
                        existing_row = sheet_dfs[sheet_name][
                            (sheet_dfs[sheet_name]["XML Tag"] == xml_tag) &
                            (sheet_dfs[sheet_name]["Context Reference"] == context_ref)
                        ]
                        if existing_row.empty:
                            new_row = {
                                "XML Tag": xml_tag,
                                "Context Reference": context_ref,
                                "Extracted Value": extracted_value,
                                "Xform Flag": Xform_Flag,
                                "XFrom Transformation": XFrom_Transformation
                            }
                            new_row_df = pd.DataFrame([new_row])
                            new_row_df = conditon_on_df(new_row_df)
                            sheet_dfs[sheet_name] = pd.concat([sheet_dfs[sheet_name], new_row_df], ignore_index=True)

    # Save the final output Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, df in sheet_dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        for sheet_name, missing_df in missing_dfs.items():
            missing_df.to_excel(writer, sheet_name=sheet_name, index=False)
        if not extra_tags.empty:
            extra_tags.to_excel(writer, sheet_name="Extra_Tags_from_XML", index=False)
        if not merged_df_1.empty:
            merged_df_1.to_excel(writer, sheet_name="Extra_Context_Refs_from_XML", index=False)

    print(f"Processed data saved to {output_path}")
    return sheet_dfs
