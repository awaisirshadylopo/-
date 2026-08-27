LISTING_ATTRIBUTE_QUERY = """
select 
 t.source_id as source_id,
 t.batch_id as batch_id,
 t.target_listing_id as listing_id,
 has_swimming_pool,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(swimming_pool_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as swimming_pool_info,
 is_waterfront      ,
 has_garage      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(garage_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as garage_info,
 conventional_financing      ,
 has_water_view      ,
 has_basement      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(basement_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as basement_info,
 has_central_air_conditioning      ,
 has_granite_counters      ,
 has_hardwood_floors      ,
 is_gated_community      ,
 has_fenced_yard      ,
 has_boat_slip      ,
 has_carport      ,
 has_garage_attached      ,
 has_garage_detached      ,
 is_parking_covered      ,
 is_pool_above_ground      ,
 has_fireplace      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(fireplace_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as fireplace_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(home_access_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as home_access_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_community_features_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_community_features_info,
 has_community_pool      ,
 is_tennis_community      ,
 is_senior_community      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_appliances_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_appliances_info,
 has_inside_laundry      ,
 has_hot_tub      ,
 mls_association_info      ,
 has_barn      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(barn_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as barn_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_cooling_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_cooling_options_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(driveway_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as driveway_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(environmental_issues_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as environmental_issues_info,
 has_deck_porch      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(deck_porch_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as deck_porch_info,
 has_storage_bldgs,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_exterior_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_exterior_options_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(exterior_material_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as exterior_material_info,
 is_brick      ,
 is_wood_construction      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(farm_equipment_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as farm_equipment_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(fenced_yard_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as fenced_yard_info,
 is_cash_financing      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_financing_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_financing_options_info,
 is_fha_qualified      ,
 has_lease_option      ,
 has_trade_or_exchange_option ,
 has_gas_heat      ,
 has_electric_heat      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_heating_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as  other_heating_options_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_heating_types_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_heating_types_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_lot_features_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_lot_features_info,
 has_rv_boat_parking      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(roof_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as roof_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sewer_type_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sewer_type_info,
 is_handicap_equipped      ,
 is_fixer_upper      ,
 is_contemporary_style      ,
 is_traditional_style      ,
 is_bungalow_style      ,
 is_cape_style      ,
 is_colonial_style      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_home_styles_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_home_styles_info,
 is_victorian_style      ,
 is_tudor_style      ,
 has_loft      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(hoa_fee_period_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as hoa_fee_period_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(hoa_fee_includes_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as hoa_fee_includes_info,
 case when hoa_fee_info is null then null else cast(concat(concat('{{', REPLACE(REPLACE(REPLACE(hoa_fee_info,',None',''),', None',''),'None,','')),'}}') as numeric[]) end as hoa_fee_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(builder_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as builder_info,
 is_high_rise      ,
 is_new_construction      ,
 is_manufactured      ,
 is_short_sale      ,
 has_sprinkler_system      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(patio_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as patio_info,
 has_elevator      ,
 has_lake_view      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_floor_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_floor_options_info,
 is_green_energy_efficient      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(green_energy_efficient_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as green_energy_efficient_info,
 is_horse_property      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(horse_property_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as horse_property_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_interior_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_interior_options_info,
 has_walkin_closet      ,
 has_cathedral_ceiling      ,
 has_wet_bar      ,
 is_conventional_financing      ,
 has_owner_financing_option      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_parking_options_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_parking_options_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(restrictions_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as restrictions_info,
 is_corner_lot      ,
 is_on_cul_de_sac      ,
 has_golf_course_view      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(waterfront_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as waterfront_info,
 is_multifamily      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(commercial_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as commercial_info,
 has_city_view      ,
 has_river_view      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(water_type_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as water_type_info,
 has_great_room      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(laundry_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as laundry_info,
 is_foreclosure      ,
 is_furnished      ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(furnished_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as furnished_info,
 has_dock      ,
 has_covered_parking      ,
 has_attic,
 has_den,
 has_dock_permit,
 has_family_room,
 has_formal_dining_room,
 has_kitchen,
 has_living_room,
 has_master_bath,
 has_master_bedroom,
 is_ranch,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(ranch_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as ranch_info,
 has_security_system,
 has_va_financing,
 has_virtual_tour,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(virtual_tour_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as virtual_tour_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(distance_from_beach_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as distance_from_beach_info,
 has_beach_access,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(beach_access_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as beach_access_info,
source_creation_date      ,
source_last_update_date      ,
t.y_creation_date      ,
y_last_update_date   ,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(asbestos_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as asbestos_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(flood_insurance_required_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as flood_insurance_required_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(flood_zone_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as flood_zone_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(foundation_materials_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as foundation_materials_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(beach_ownership_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as beach_ownership_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(tax_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as tax_info,
 is_inside_subdivision,
 has_foyer,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_features_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_features_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(dim_family_room_info,'None',''),', ',','),',,',',')),',')),''),',') as dim_family_room_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(dim_kitchen_info,'None',''),', ',','),',,',',')),',')),''),',') as dim_kitchen_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(dim_living_room_info,'None',''),', ',','),',,',',')),',')),''),',') as dim_living_room_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(dim_master_bedroom_info,'None',''),', ',','),',,',',')),',')),''),',') as dim_master_bedroom_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(dock_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as dock_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(kitchen_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as kitchen_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(security_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as security_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(solar_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as solar_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(level_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as level_info,
 is_golf_course_lot,
 is_pool_inground,
 has_first_floor_master,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(mile_marker_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as mile_marker_info,
 is_golf_course_community,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(community_amenities_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as community_amenities_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(interior_floor_plan_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as interior_floor_plan_info,
 pets_allowed,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(water_access_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as water_access_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(water_extras_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as water_extras_info,
 string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(water_view_desc_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as water_view_desc_info,
 is_reo_bank_owned,
 has_hoa_fees,
 has_cdd_fees,
 is_pool_indoor,
 is_log_home,
 has_first_floor_bedroom,
 has_in_law_suites,
 is_farm_house,
 has_screen_porch,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(dining_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as dining_room_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(living_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as living_room_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(family_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as family_room_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(foyer_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as foyer_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(master_bedroom_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as  master_bedroom_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(master_bathroom_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as master_bathroom_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(energy_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as energy_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(accessibility_features,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as accessibility_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(window_features,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as window_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(utilities_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as utilities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(hot_water_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as hot_water_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(fuel_type_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as fuel_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(extra_features_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as extra_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(master_bath_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as master_bath_info,
  is_remodeled,
  has_outbuildings,
  has_vaulted_ceilings,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(view_desc_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as view_desc_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(hvac_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',')as hvac_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(guest_accommodations,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as guest_accommodations_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(garage_spaces_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as garage_spaces_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(pets_allowed_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as pets_allowed_info,
has_no_hoa_fees,
has_rv_gate,
has_rv_parking,
has_rv_garage,
is_one_story,
is_two_stories,
is_three_plus_stories,
has_dining_room,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(bathroom_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as bathroom_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(bedroom_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as bedroom_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(other_structures,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as other_structures_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(zoning_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as zoning_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(construction_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as construction_info,
is_hud_owned,
has_workshop,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(workshop_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as workshop_info,
is_pool_outdoor ,
has_community_clubhouse,
has_membership_fee,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(membership_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as membership_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(property_description,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as property_desc_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(attic_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as attic_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(ski_property_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as ski_property_info,
is_ski_property,
is_exterior_stone,
is_exterior_stucco,
has_exterior_wood_frame,
is_exterior_concrete_block,
has_exterior_siding,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(exclusions_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as exclusions_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(inclusions_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as inclusions_info,
has_stone_floors ,
has_ceramic_floors,
has_marble_stone_floors,
has_beach_club,
has_gulf_access,
club_membership_req,
is_flood_zone,
has_deed_restrictions,
is_sale_auction,
is_sale_auction_reo,
is_sale_auction_short_sale,
is_villa_style,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(originating_mls_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as originating_mls_info,
is_brownstone_style,
is_english_style,
is_french_provincial_style,
is_greystone_style,
is_row_house,
is_cottage,
is_georgian_style,
is_spanish_style,
is_queen_anne_style,
is_american_style,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(gulf_access_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as gulf_access_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(community_type_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as community_type_info,
is_fee_simple_ownership,
is_lease_hold_ownership,
has_driveway,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(spa_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as spa_info ,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(elevator_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as elevator_info,
has_exterior_hardie_plank ,
has_waterfront_beach,
has_waterfront_deep_water ,
has_waterfront_tidal_creek,
has_waterfront_ocean_view ,
has_waterfront_marsh ,
has_waterfront_marsh_view,
no_community_deed_restrictions,
has_tile_floors,
is_waterfront_salt_water,
is_waterfront_fresh_water,
has_city_taxes	,
has_condo_fees	,
stories ,
garage_spaces ,
hoa_fee_amt_per_month ,
lot_sqft,
is_not_senior_community,
baths_half,
baths_full,
blocks_to_ocean,
has_main_floor_master_bdrm,
has_main_floor_bdrm,
has_boat_lift,
has_generator,
has_home_automation,
has_privacy_wall,
is_boating_community,
is_deeded,
is_oversize_lot,
is_turnkey,
has_dual_master_bedrooms,
has_shared_dock,
is_first_seller_carry,
is_wraparound_financing,
is_one_plus_half_story,
is_usda_financing,
has_jetty_view ,
has_second_kitchen ,
has_second_master_bedroom ,
has_sound_view ,
has_strait_view ,
has_territorial_view ,
is_city_lot,
has_no_membership_fee,
has_garden_style_patio,
is_partial_ownership,
has_no_deed_restrictions,
has_shop,
has_pole_barn,
is_multi_level,
is_split_foyer
,is_new_listing
,has_spa
,is_a_frame_style
,is_art_deco_style
,is_arts_crafts_style
,is_bi_level_style
,is_bueax_arts_style
,is_cabin_style
,is_carriage_house_style
,is_chalet_style
,is_craftsman_style
,is_dome_style
,is_federal_style
,is_french_country_style
,is_international_style
,is_manor_style
,is_post_beam_style
,is_prairie_style
,is_raised_rambler_style
,is_raised_rancher_style
,is_rambler_style
,is_rancher_style
,is_salt_box_style
,is_split_level_style
,is_transitional_style
,is_provincial_style
,	has_built_in_bbq
,	has_bonus_game_room
,	has_circular_driveway
,	has_community_laundry_room
,	has_community_media_room
,	has_community_playground
,	has_community_spa
,	has_covered_patio
,	has_exercise_room
,	has_gazebo
,	has_library
,	has_media_room
,	has_patio
,	has_childrens_play_area
,	has_private_street
,	has_private_tennis_court
,	has_private_yard
,	has_hand_racquetball_courts
,	has_separate_guest_house
,	has_sport_courts
,	has_storage_shed
,	has_horse_facility
,	is_complete_spec_home
,	property_desc_alley
,	property_desc_hillside_lot
,	is_historic_home
,	street_not_paved
,has_heat_pump
,is_retirement_community
,has_central_heating
,has_land_contract
,has_no_cdd_fees
,is_not_short_sale
,is_lender_owned
,has_carpet_floors
,tax_amt
,is_land_lease
,is_not_land_lease
,is_construction_status_built
,is_construction_status_under_construction
,is_construction_status_to_be_built
,monthly_condo_fees
,has_water_access
,has_basement_finished
,has_basement_partially_finished
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(short_sale_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as short_sale_info
,is_ccr_lot
,is_wooded_lot
,has_ceramic_counters
,has_corian_counters
,has_formica_counters
,has_laminate_counters
,has_stone_counters
,has_tile_counters
,has_beamed_ceiling
,has_high_ceiling
,has_trey_ceiling
,has_two_story_ceiling
,has_coffered_ceiling
,has_casement_windows
,has_louvered_windows
,has_low_emissivity_windows
,has_palladian_windows
,has_plantation_shutters
,has_roller_shields
,has_atrium_windows
,has_tinted_windows
,has_wood_frame_windows
,has_french_mullioned_windows
,has_garden_windows
,has_insulated_windows
,has_custom_covering
,has_double_pane_windows
,has_drapes_curtains
,has_energy_star_windows
,has_triple_pane_windows
,has_bay_window
,has_blinds
,has_screens
,has_shutters
,has_skylights
,has_solar_screens
,has_solar_tinted_windows
,has_stained_glass
,has_storm_windows
,has_atrium_doors
,has_double_door_entry
,has_energy_star_doors
,has_french_doors
,has_insulated_doors
,has_mirrored_closet_doors
,has_panel_doors
,has_service_entrance
,has_sliding_glass_doors
,has_storm_doors
,has_solar_panels
,is_handyman_special
,has_two_units_on_lot
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(other_rooms_info,'None',''),', ',','),',,',',')),',')),''),',') as other_rooms_info
,has_bedrooms_plus
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedrooms_plus_info,'None',''),', ',','),',,',',')),',')),''),',') as bedrooms_plus_info
,has_laminate_flooring
,has_forced_air
,is_fmha_rural_dev_financing
,has_sun_room
,bedrooms_plus
,is_gated_with_guard
,mello_roos_fee_amt
,is_garden_style
,is_townhouse
,bldg_sq_ft
,has_downstairs_bedroom
,is_coop_ownership
,has_outdoor_kitchen
,is_cape_cod_style
,has_ac_unit
,has_accessory_dwelling_unit
,lot_size_sqft_range
,is_mediterranean_style
,is_other_style
,is_exterior_aluminium_vinyl
,is_exterior_shingle
,is_exterior_other
,has_parking_tandem_private
,has_parking_tandem_shared
,has_parking_other
,has_parking_none
,has_appliance_dishwasher
,has_appliance_compactor
,has_appliance_disposal
,has_appliance_microwave
,has_appliance_refrigerator
,has_appliance_washer_dryer
,has_appliance_oven_range_gas
,has_appliance_oven_range_electric
,has_appliance_none
,has_appliance_other
,has_intercom
,has_terrace
,is_half_duplex_style
,is_patio_zero_lot_style
,is_tri_level_style
,is_twin_home_style
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(legal_desc_info,'None',''),', ',','),',,',',')),',')),''),',') as legal_desc_info
,is_hoa_period_monthly
,is_hoa_period_quarterly
,is_hoa_period_semi_annually
,is_hoa_period_yearly
,application_fee_amt
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(maintenance_fee_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as maintenance_fee_includes_info
,is_hoa_type_condo
,is_hoa_type_homeowners
,is_hoa_type_none
,is_hoa_type_other
,has_back_yard_fenced
,has_back_yard_green_space
,has_back_yard
,is_not_fenced
,has_outdoor_fireplace
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(full_bathroom_info,'None',''),', ',','),',,',',')),',')),''),',') as full_bathroom_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(partial_bathroom_info,'None',''),', ',','),',,',',')),',')),''),',') as partial_bathroom_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(transaction_type_info,'None',''),', ',','),',,',',')),',')),''),',') as transaction_type_info
,has_storm_shutters
,has_wheelchair_access
,maint_fee_amt_per_month
,has_basement_unfinished
,has_basement_full
,has_basement_partial
,has_full_bath_level_1
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(county_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as county_info
,has_mountain_view
,has_valley_view
,has_hill_view
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(building_name_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as building_name_info
,has_brick_floors
,has_concrete_floors
,has_vinyl_floors
,has_greenbelt
,has_balcony
,has_kitchen_island
,has_kitchen_pantry
,is_builder_owned
,is_antebellum_style
,has_basement_walkout
,is_exterior_vinyl
,is_construction_frame
,has_guest_house_attached
,has_heat_forced_air
,has_natural_gas_heat
,has_appliance_washer
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(lot_dimensions_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as lot_dimensions_info
,has_meadow_view
,has_ski_slopes_view
,has_south_facing_view
,has_waterfall_view
,has_woods_view
,has_public_sewer
,has_public_water
,has_private_water
,has_private_sewer
,is_seller_finance
,regime_fee_amt_per_month
,has_tennis_court_view
,is_ocean_oriented
,is_sound_oriented
,is_age_restricted
,has_broadband_internet
,has_central_vacuum
,has_sprinklers_pressurized_irrigation
,has_view
,has_corral_lot
,is_renovated
,has_irrigation
,is_id_housing_finance
,is_in_probate
,has_border_public_land
,has_main_level_parking
,has_main_level_laundry
,has_dock_unrestricted
,has_dock_restricted
,is_near_park
,is_mid_century_style
,is_near_public_transport
,is_ski_in
,is_ski_out
,has_hoa_utilities_included
,is_semi_detached
,is_south_of_rr_track
,is_north_of_rr_track
,has_county_taxes
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(subdivision_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as subdivision_info
,is_property_attached
,is_property_detached
,land_lease_range
,has_parking_type_dirt
,has_parking_type_gravel
,has_parking_type_uncovered
,has_parking_type_street
,has_parking_type_concrete
,has_master_downstairs
,has_master_dressing_room
,has_master_efficiency
,has_master_remote
,has_master_suite
,is_comunity_dry_dock
,is_siding_aluminum
,is_exterior_brick_3_side
,is_exterior_brick_4_side
,is_siding_wood
,has_community_security
,is_not_new_construction
,has_parking_1_car
,has_parking_2_cars
,has_parking_3_plus_cars
,has_sewer
,has_sewer_cesspool
,has_sewer_septic
,has_water_source_shared_well
,has_water_source_private_well
,has_contingency_inspection
,has_split_bedroom_plan
,is_end_unit
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(contingency_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as contingency_info
,has_driveway_gate
,has_automatic_gate
,is_walk_to_slopes
,is_on_bus_route
,is_in_town
,is_short_term_rental_allowed
,has_separate_tub_shower
,has_breakfast_area
,has_upstairs_laundry
,has_solid_counters
,has_double_ovens
,basement_crawl_space
,has_front_porch
,is_lot_wetlands
,is_lot_stream
,is_lot_pond
,is_lot_high
,is_lot_low
,has_waterfront_shallow
,is_lot_rolling
,is_lot_pond_site
,is_lot_level
,basement_walkout_level
,has_dock_multiple_slips
,has_dock_single_slip
,has_dock_none
,has_dock_other
,has_dock_pier
,has_dock_against_bullhead
,is_within_historic
,sewer_has_grinder_pump
,sewer_has_holding_tank
,sewer_has_mound_system
,water_has_tap_fee
,water_has_well
,has_community_assigned_parking
,has_community_day_care
,community_has_dock
,community_has_game_room
,community_has_laundry_facility
,community_has_marina
,community_has_satellite_tv
,sewer_is_public_central
,sewer_is_private_central
,water_is_public_central
,water_is_private_central
,num_of_units
,is_parking_off_street
,is_not_flood_zone
,lot_lake_frontage 
,is_two_plus_stories 
,is_sold_as_is	 
,is_property_mid_rise_up_to_5_stories 	 
,is_investor_owned 	 
,is_frontage_river 	 
,is_frontage_lake 	 
,is_european_style 	 
,has_pool_gunite 	 
,has_pond  
,has_level_driveway  
,has_back_yard_private 	 
,has_2_story_foyer  
,community_has_sidewalk 
,community_has_country_club
,is_condition_as_is
,has_no_pet_restrictions
,is_dock_rackominium
,is_condo_hotel
,is_style_coop
,is_style_condo
,is_style_townhouse
,is_style_garage
,has_contingency
,is_sloped_lot
,is_railed_lot
,is_public_maintained_road_lot
,is_open_lot
,is_lot_private_road
,is_lot_no_backyard_grass
,is_lot_lakefront
,is_lot_irregular
,is_lot_interior
,is_lot_backs_to_greenbelt
,is_lot_backs_to_golf_course
,is_lot_alley_access
,is_lake_on_lot
,is_flag_lot
,is_drought_tolerant_landscaping_lot
,is_curbs_lot
,has_view_pond
,has_view_panoramic
,has_view_hill_country
,is_active
,is_active_option_contract
,is_active_kick_out
,is_office
,is_handicap_accessible
,has_covered_porch
,is_ski_in_ski_out
,is_cross_country_skiing
,has_view_fields
,has_view_creek
,is_partially_cultivated_lot
,is_cultivated_lot
,is_canal_lot
,is_duplex_style
,is_active_call_agent
,pets_small_21_30_lbs
,pets_very_small_0_20_lbs 
,arch_style_mountain_contemporary
,arch_style_rustic_contemporary
,arch_style_urban_contemporary
,has_waterfront_bay
,has_waterfront_bog
,has_waterview_bog
,has_waterview_harbor
,has_waterview_marsh
,walk_to_freshwater
,walk_to_saltwater
,walk_to_water
,arch_style_loft
,arch_style_patio
,is_story_one_w_upper_bonus_room
,has_back_load_garage
,has_basement_garage
,has_cooling_attic_ventilator
,has_active_solar
,has_boiler
,has_circulator
,has_circulator_hot_water
,has_cooling_attic_fan
,has_cooling_chiller
,has_garage_tandem
,has_home_theater
,has_wine_cellar
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(development_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as development_info
,is_senior_community_55_plus
,is_low_rise
,is_villa_detached_style
,is_villa_attached_style
,has_private_dock
,waterfront_no_fixed_bridges
,has_flood_insurance
,has_heat_bottle_gas
,has_heat_coal
,has_heat_geothermal
,has_heat_kerosene
,has_heat_none
,has_heat_other
,has_heat_solar
,has_heat_wood
,has_mooring
,has_river
,is_recreation_style
,is_residential_style
,is_retail_style
,is_services_style
,is_side_side_style
,is_single_family_attached_style
,is_slip_style
,is_style_mobile_home
,is_style_modular
,is_total_residential_development_style
,is_type_agriculture
,is_type_apartment
,is_type_auto_related
,is_type_beauty_service
,is_type_bed_and_breakfast
,is_type_below_ground_unit
,is_type_commercial
,is_type_distributor
,is_type_dock
,is_type_food_beverage
,is_type_health_care
,is_type_hotel_motel
,is_type_industrial
,is_type_liquor_store
,is_type_mixed
,is_up_down_style
,is_walk_to_fresh_water
,is_walk_to_salt_water
,is_warehouse_style
,is_wholesale_style
,has_solar_thermal_collection
,has_energy_star
,has_air_source_heat_pump
,has_hot_air_gravity
,has_hot_water_baseboard
,has_electric_baseboard
,has_hot_water_radiators
,has_steam
,has_radiant
,has_space_heater
,has_floor_furnace
,has_humidifier
,has_propane
,has_wood
,has_extra_flue
,has_hydro_air
,has_geothermal_heat_source
,has_ground_source_heat_pump
,has_hydronic_floor_heat
,has_passive_solar
,has_waterfront_dock
,is_waterfront_frontage
,is_waterfront_walk_to
,has_waterfront_direct_access
,has_waterfront_navigable
,is_waterfront_public
,is_waterfront_private
,has_style_colonial_revival
,has_style_neoclassical
,has_style_octagon
,has_style_italianate
,has_style_dutch_colonial
,has_style_french_colonial
,has_style_gothic_revival
,has_style_second_empire
,has_style_garrison
,is_split_entry_style
,is_gambrel_style
,is_antique_style
,is_front_to_back_style
,is_lofted_split_style
,is_greek_revival_style
,is_shingle_style
,is_location_midtown
,has_community_sewer
,pets_med_31_40_lbs
,pets_large_40_lbs
,is_location_waterfront
,is_property_sub_type_agricultural
,is_property_sub_type_cluster
,is_property_sub_type_commercial
,is_property_sub_type_condo
,is_property_sub_type_duplex
,is_property_sub_type_industrial
,is_property_sub_type_institutional
,is_property_sub_type_manufactured
,is_property_sub_type_modular
,is_property_sub_type_quad
,is_property_sub_type_stick
,is_property_sub_type_triplex
,is_property_sub_type_undeveloped
,has_storm_shelter
,is_half_acre_plus
,has_garage_one_and_half_car
,has_garage_two_and_half_car
,has_garage_5_cars
,has_garage_6_plus
,is_eleven_plus_stories
,is_one_and_three_fourth_stories
,is_one_and_one_fourth_stories
,is_one_story_ground
,is_one_story_up
,is_two_and_half_story
,is_four_to_ten_story
,is_quad_level
,is_triplex_style
,is_fourplex_style
,num_fireplaces
,has_downstairs_bathroom
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sold_terms_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sold_terms_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(downstairs_bath_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as downstairs_bath_info
,single_fam_att_ohana
,single_fam_det_ohana
,is_minimum_lease_one_month
,is_minimum_lease_one_to_two_years
,is_minimum_lease_one_to_seven_days
,is_minimum_lease_one_week
,is_minimum_lease_two_months
,is_minimum_lease_two_weeks
,is_minimum_lease_two_plus_years
,is_minimum_lease_three_months
,is_minimum_lease_four_months
,is_minimum_lease_five_months
,is_minimum_lease_six_months
,is_minimum_lease_seven_months
,is_minimum_lease_eight_to_twelve_months
,is_minimum_lease_none
,is_no_rental_allowed
,has_waterfront_boat_house
,has_exterior_cabana
,has_covered_deck
,has_exterior_greenhouse
,has_grill
,has_exterior_Lake
,has_exterior_deck_open
,has_exterior_patio_open
,has_exterior_pond
,has_stable
,has_pool_concrete
,has_pool_fiberglass
,has_pool_vinyl_lined
,is_lot_campus
,is_lot_creek
,is_lot_infill
,is_lot_pasture
,is_lot_rural
,is_lot_riverfront
,is_lot_section_line
,is_lot_waterview
,has_no_special_sale_conditions
,has_optional_hoa_fees
,is_shelter_safe_room
,is_shelter_garage_floor
,is_shelter_outdoors
,is_shelter_yes
,is_shelter_no
,is_occupied
,is_not_occupied
,is_oven_elec
,is_oven_gas
,has_no_city_taxes
,has_hopa_no
,has_hopa_yes_verified
,has_contingency_with_bump_clause
,has_contingency_sale_of_another_property
,has_contingency_third_prty_approval
,has_contingency_without_bump_clause
,has_contingency_subject_to_statutory_rescission
,parking_spaces
,tax_year
,total_annual_recur_fees
,is_dutch_colonial_style 
,is_earth_sheltered_style 
,is_mobile_home_style
,is_barn_style
,is_four_square_style
,is_converted_barn_style
,is_apartment_style
,is_estate_style
,is_flats_style
,is_garden_apartment_style
,is_mini_estate_style
,is_mobile_home_with_property_style
,is_seasonal_style
,is_splanch_style
,is_three_sides_style
,is_three_updown_style
,is_four_sides_style
,is_four_up_down_style
,is_chateau_style
,is_french_style
,is_garden_ranch_style
,is_hi_ranch_style
,is_normandy_style
,is_penthouse_style
,is_single_family_detached_style
,is_studio_apartment_style
,is_units_on_different_floors_style 
,is_side_by_side_style
,is_not_reo_bank_owned
,is_not_gated_community
,is_not_lender_owned
,is_hoa_type_mandatory
,is_hoa_type_voluntary
,has_canyon_view
,has_city_lights_view
,has_coastline_view
,has_marina_view
,has_ocean_view
,has_pool_view
,has_tree_top_view
,has_trees_view
,has_white_water_view
,is_architectural_style
,is_calif_bungalow_style
,is_hacienda_style
,is_modern_style
,has_no_view
,has_greenbelt_view
,has_garden_space
,has_fenced_fully
,has_sprinkler_system_auto
,carport_spaces
,is_private_ownership
,is_bank_ownership
,is_government_ownership
,is_two_plus_half_story
,is_level_none
,is_level_other
,is_atrium_style
,is_style_double_wide
,is_earth_home_style
,is_style_other
,is_government_financing
,is_lot_backs_to_public_grnd
,is_lot_backs_to_comm_grnd
,is_lot_backs_to_trees
,is_lot_fenced_chain_link
,is_lot_dock
,is_lot_electric_fence
,is_lot_fencing
,is_lot_adjoins_government_land
,is_lot_suitable_for_horses
,is_lot_infill_lot
,is_lot_fence_invisible_pet
,is_lot_lake_access
,is_lot_park_adjacent
,is_lot_pasture_land
,is_lot_pie_shaped_lot
,is_lot_partial_fencing
,is_lot_park_view
,is_lot_river
,is_lot_sidewalk
,is_lot_spring
,is_lot_streetlights
,is_lot_terraced
,is_lot_waterfront_lot
,is_lot_wood_fenced
,is_lot_wooven_wire_fence
,is_lot_water_view
,is_lot_backs_to_open_grnd
,is_modified_two_story
,is_three_level_split_style
,is_four_plus_level_split_style
,is_style_split_entry
,is_quad_style
,is_detached
,is_two_unit
,is_converted_mansion_style
,is_time_share
,has_study
,is_bankruptcy_property
,has_notice_of_default
,is_standard_listing
,has_listing_condition_third_party_approval
,is_property_type_detached_with_commercial_elements
,is_property_type_business
,is_property_type_farm
,is_property_type_triplex
,is_property_type_link
,is_property_type_multiplex
,is_property_type_fourplex
,is_property_type_other
,is_property_type_rural_residential
,is_property_type_semi_detached
,is_property_type_vacant_land
,is_property_type_mobile
,is_property_type_store_with_apartment
,is_property_type_apartment_unit
,is_property_type_land
,is_property_type_modular
,has_doorman
,is_setting_street
,is_setting_greenbelt
,is_setting_lakefront
,is_setting_split_lakefront
,is_setting_golf
,is_setting_river_creek
,is_setting_condo_project
,is_setting_lakefront_condo_project
,is_setting_common_area
,is_setting_ski_trail
,has_fireplace_in_basement
,has_fireplace_in_great_room
,has_fireplace_in_living_room
,has_fireplace_in_master_bedroom
,has_propane_gas_heat
,is_electric_available
,is_electric_on_site
,has_generator_auto_start
,is_link
,has_ground_source_heat
,has_oil_source_heat
,has_propane_source_heat
,has_solar_source_heat
,has_wood_source_heat
,has_sewer_none
,has_basementtype_apartment
,has_basementtype_finished_walk_out
,has_basementtype_half
,has_basementtype_separate_entrance
,has_fireplace_stove
,has_front_yard_driveway
,has_lane_driveway
,has_mutual_driveway
,has_private_driveway
,has_private_double_driveway
,has_right_of_way_driveway
,has_garage_builtin
,is_arch_style_apartment
,is_arch_style_studio
,is_backsplit_3_style
,is_backsplit_4_style
,is_backsplit_5_style
,is_bungaloft_style
,is_bungalow_raised_style
,is_sidesplit_3_style
,is_sidesplit_4_style
,is_sidesplit_5_style
,has_lot_front_ft
,is_waterfront_direct
,is_waterfront_indirect
,is_condo_townhouse
,is_parking_underground
,condo_unit_level
,is_shared_frontage
,has_access_public_access_one_mile_or_less
,is_private_frontage
,is_water_extras_no_wake
,is_deeded_boat_lot
,is_deeded_access
,is_water_channel
,has_assoc_access
,is_water_all_sport
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_13_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bath_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bath_13_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_number_of_bed_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_number_of_bed_13_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_projected_total_monthly_rent_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_projected_total_monthly_rent_13_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_total_monthly_rent_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_total_monthly_rent_13_info
,has_outbuilding_green_house
,has_outbuilding_hangar
,has_outbuilding_boat_house
,has_outbuilding_guest_house
,has_outbuilding_other
,has_outbuilding_stable
,is_not_waterfront
,is_exterior_aluminum
,is_exterior_asbestos
,is_exterior_brick_and_frame
,is_exterior_earth_sheltered
,is_exterior_efis
,is_exterior_log
,is_exterior_masonry_vaneer
,is_construction_not_mobile_home
,is_exterior_steel
,is_exterior_tilt_up
,is_construction_underground
,is_exterior_veneer
,has_basement_concrete_block 
,has_basement_day 
,has_basement_drain_tiled 
,has_basement_drainage_system 
,has_basement_egress_window 
,has_basement_insulating_concrete_forms 
,has_basement_none
,has_basement_poured_concrete 
,has_basement_slab 
,has_basement_stone 
,has_basement_sump_pump
,has_basement_wood
,num_of_floors
,unit_floor_location
,total_num_of_beds
,is_not_foreclosure
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(lake_waterway_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as lake_waterway_info
,has_lake_lakerights
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(lake_waterway_name,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as lake_waterway_name
,is_waterfront_eau_gallie_river
,is_waterfront_horse_creek
,is_waterfront_honeymoon_lake
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unbranded_virtual_tour_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unbranded_virtual_tour_info
,is_spanish_colonial
,is_tuscan
,is_unfurnished
,has_no_electric_heat
,is_not_distressed_property
,is_distressed_property
,is_end_unit_townhouse
,is_waterfront_community_no_access
,is_barn
,is_traditional_ranch
,is_shore_colonial_style
,is_new_classisism
,is_english_manor_style
,is_cotswold
,is_colonial_ranch
,has_outbuilding_carriage_house
,has_outbuilding_cottage_with_kitchen
,has_outbuilding_garage_apartment
,has_outbuilding_gatehouse
,has_outbuilding_pool_house
,has_outbuilding_possible
,has_outbuilding_potting_shed
,has_outbuilding_shed
,has_outbuilding_staff_quarters
,has_outbuilding_studio
,is_min_lease_one_week
,is_min_lease_one_month
,is_min_lease_one_year
,is_min_lease_seven_months
,has_roof_shingle_asphalt
,is_regency_style
,is_georgian_colonial_style
,is_condo_type_flat
,is_condo_ground_level
,is_condo_building_two_stories
,condo_fees
,is_condo_fee_freq_annual
,is_condo_fee_freq_semi_annual
,is_condo_fee_freq_quarterly
,is_condo_fee_freq_monthly
,lake_waterway
,is_powerboats_allowed
,is_powerboats_non_allowed
,has_dock_slip_included
,has_dock_slip_available
,price_per_sqft
,is_one_fourth_acre_plus
,is_one_acre_plus
,is_two_acre_plus
,has_basement_daylight
,has_basement_shelf
,basement_percent_finished
,master_hoa_fee_amt_per_month
,is_condo_period_annually
,is_condo_period_monthly
,is_condo_period_quarterly
,is_condo_period_semi_annually
,mandatory_club_fee_amt
,lot_front
,water_frontage
,is_master_hoa_period_annually
,is_master_hoa_period_monthly
,is_master_hoa_period_quarterly
,is_master_hoa_period_semi_annually
,is_pool_on_lot
,is_pool_private_association
,is_lot_has_tree_mature
,is_lot_has_trees_small
,is_lot_on_trail
,is_level_one_level_plus_loft
,has_master_suite_street_lvl
,has_side_yard_access
,has_family_park
,waterview_direction
,waterview_direction_E
,waterview_direction_SE
,waterview_direction_S
,waterview_direction_SW
,waterview_direction_W
,waterview_direction_NW
,waterview_direction_N
,waterview_direction_NE
,has_pool_no_private_pool
,has_pool_no_community_pool
,is_single_level_unit_style
,is_two_story_split_style
,is_five_level_split_style
,has_association_pool
,is_zoning_horse_property
,is_zoning_vacation_rental
,is_on_reservoir
,is_zoning_commercial
,has_casita
,has_basement_suite
,is_zone_cc
,is_zone_e
,is_zone_n
,is_zone_ne
,is_zone_nw
,is_zone_s
,is_zone_se
,is_zone_w
,is_construction_CBS
,is_construction_modular
,is_partially_furnished
,is_coming_soon
,has_office
,has_waterfront_semi_oceanfront_second_row
,has_waterfront_semi_oceanfront_third_row
,has_waterfront_semi_oceanfront_fourth_row
,has_waterfront_semi_oceanfront_fifth_row
,has_waterfront_semi_soundfront
,insurance_paid
,is_two_on_a_lot
,has_aux_dwelling_unit
,is_above_flood
,is_stilt_column_height_short
,is_stilt_column_height_medium
,has_any_view
,has_back_bay_view
,has_bay_view
,has_bridge_view
,has_canal_view
,has_catalina_view
,has_courtyard_view
,has_desert_view
,has_estuary_view
,has_harbor_view
,has_lagoon_view
,has_lake_front_view
,has_landmark_view
,has_orchard_view
,has_other_view
,has_park_view
,has_pasture_view
,has_peek_a_boo_view
,has_pier_view
,has_reservoir_view
,has_rocks_view
,has_vineyard_view
,has_bluff_view
,has_community_has_walk_path
,is_pets_cats_allowed
,is_pets_dogs_allowed
,has_pets_number_limit
,is_no_pets_allowed
,has_pets_weight_and_height_limit
,garage_carport_spaces
,has_parking_deeded
,has_driver_under_main_level_parking
,has_workshop_w_electricity
,is_owner_will_carry_terms
,has_park_type_family
,has_park_type_senior
,has_water_source_city
,has_waterfront_gulf
,year_remodeled
,is_last_change_new_listing
,is_last_change_back_on_market
,is_last_change_active_with_contract
,is_last_change_price_decrease
,is_last_change_price_increase
,num_fam
,has_pool_saltwater
,has_pool_waterfall
,has_pool_pebble
,is_floorplan_open
,has_two_staircases
,is_floorplan_in_law
,has_interior_recessed_lighting
,has_ceiling_fan
,has_cooling_zoned
,has_cooling_whole_house_fan
,has_quartz_counters
,has_kitchen_remodeled
,has_upper_level_laundry
,has_guest_maids_quarters
,has_utility_room
,has_walk_in_pantry
,has_dressing_area
,has_all_bedrooms_up
,has_separate_family_room
,has_kitchen_convection_oven
,has_kitchen_range_builtin
,has_bbq_private
,has_exterior_lighting
,has_24_hour_security
,is_lot_desert_front
,is_lot_garden
,is_lot_horse_property_improved
,is_lot_landscaped
,is_foothills_community
,is_mountainous_community
,is_preserve_community
,is_horse_trails_community
,is_street_lights_community
,is_rural_community
,is_ravine_community
,is_custom_built_style
,has_assessments_special_assessments
,has_assessments_community_facility_district
,is_not_golf_course_community
,pets_has_call_for_rules
,pets_has_weight_limit
,is_land_lease_fee
,is_construction_status_model_for_sale
,is_construction_status_previously_owned
,has_locker
,has_lower_floor_master_bdrm
,has_upper_floor_master_bdrm
,has_existing_house
,is_contract_for_deed_financing
,is_creative_financing
,is_1031_exchange_financing
,is_holding_first_financing
,is_holding_second_financing
,is_lease_purchase_financing
,is_lease_back_financing
,is_seller_may_pay_close_financing
,is_private_financing_available
,land_lease_amount_annual
,has_guest_house_desc_one_bath
,has_guest_house_desc_one_bedroom
,has_guest_house_desc_two_plus_baths
,has_guest_house_desc_two_plus_bedrooms
,has_guest_house_desc_balcony
,has_guest_house_desc_cabana
,has_guest_house_desc_carport
,has_guest_house_desc_conforming
,has_guest_house_desc_efficiency
,has_guest_house_desc_garage
,has_guest_house_desc_kitchen
,has_guest_house_desc_living_room
,has_guest_house_desc_non_conforming
,has_guest_house_desc_patio
,has_guest_house_desc_screened_porch
,is_active_back_on_market
,is_active_extended
,is_active_price_change
,is_active_rfr
,num_loft
,num_den
,has_impact_windows
,is_stilt_column_height_tall
,land_lease_exp_year
,has_waterfront_fixed_bridges
,has_waterfront_seawall
,has_waterfront_canal_width_one_to_eighty
,has_waterfront_canal_width_eightyone_to_one_hundred_twenty
,has_waterfront_intracoastal
,has_waterfront_mangrove
,has_waterfront_ocean_access
,has_waterfront_point_lot
,has_waterfront_riprap
,has_waterfront_interior_canal
,has_dock_concrete
,min_rental_days
,max_rental_days
,is_farm_and_ranch
,has_basement_garage_entrance
,has_basement_inside_entrance
,has_basement_walkup
,has_fireplace_wood_burning
,has_oil_above_ground_heat
,has_oil_below_ground_source_heat
,has_heat_source_radiator
,has_heat_recovery_system
,has_hot_water
,has_cooling_geothermal
,has_cooling_high_pressure_system
,has_cooling_individual
,has_cooling_none
,has_cooling_SEER_rating_twelve_plus
,has_cooling_wall_unit
,has_cooling_window_unit
,has_heat_baseboard
,has_cooling_ductwork
,has_cooling_ductless
,has_air_purification_system
,has_municipal_water
,has_sewer_municipal
,has_sewer_other
,has_heat_zoned
,has_digital_program_thermostat
,has_heat_solar_panels_leased
,has_heat_wall_unit
,has_heat_solar_panels_owned
,has_heat_gravity
,has_heat_hot_water
,has_floor_heater
,has_cooling_other
,has_sewer_in_street
,has_water_retaining_pond
,has_sewer_city
,has_public_sewer_in_street
,has_membership_golf_bundled
,has_membership_golf_equity
,has_membership_golf_public
,has_membership_golf_non_equity
,has_subdivision_restrictions
,tax_rate
,room_desc_has_breakfast_room
,room_desc_has_den
,room_desc_has_family_room
,room_desc_has_formal_dining
,room_desc_has_formal_living
,room_desc_has_gameroom_down
,room_desc_has_gameroom_up
,room_desc_has_garage_apartment
,room_desc_has_guest_suite
,room_desc_has_guest_suite_w_kitchen
,room_desc_has_kitchen_dining_combo
,room_desc_has_1_living_area
,room_desc_has_loft
,room_desc_has_living_dining_combo
,room_desc_has_living_area_1st_floor
,room_desc_has_living_area_2nd_floor
,room_desc_has_living_area_3rd_floor
,room_desc_has_media
,room_desc_has_quarters_guest_house
,room_desc_has_study_library
,room_desc_has_sun_room
,room_desc_has_utility_room_in_garage
,room_desc_has_utility_room_in_house
,room_desc_has_wine_room
,bedroom_desc_has_1_Bedroom_Up
,bedroom_desc_has_2_Bedrooms_Down
,bedroom_desc_has_2_Master_Bedrooms
,bedroom_desc_has_All_Bedrooms_Down
,bedroom_desc_has_All_Bedrooms_Up
,bedroom_desc_has_1_Bedroom_Down_Not_Master_BR
,bedroom_desc_has_En_Suite_Bath
,bedroom_desc_has_Master_Bed_1st_Floor
,bedroom_desc_has_Master_Bed_2nd_Floor
,bedroom_desc_has_Master_Bed_3rd_Floor
,bedroom_desc_has_Master_Bed_4th_Floor
,bedroom_desc_has_Multilevel_Bedroom
,bedroom_desc_has_Sitting_Area
,bedroom_desc_has_Split_Plan
,bedroom_desc_has_Walk_In_Closet
,has_boat_ramp_private
,has_bulkhead_seawall
,has_community_waterfront_community
,has_community_boat_ramp
,is_floorplan_two_story
,is_floorplan_courtyard
,is_floorplan_efficiency
,is_floorplan_great_room
,is_floorplan_other
,is_floorplan_split_bedroom
,is_frontage_water
,has_covenants
,has_irrigation_system
,has_kitchen_breakfast_bar
,has_kitchen_instant_hot_water
,has_kitchen_island_with_cooktop
,has_kitchen_island_without_cooktop
,has_kitchen_open_to_family_room
,has_kitchen_pot_filler
,has_kitchen_pots_pans_drawer
,has_kitchen_reverse_osmosis
,has_kitchen_second_sink
,has_kitchen_soft_closing_cabinet
,has_kitchen_soft_closing_drawer
,has_kitchen_under_cabinet_lighting
,has_kitchen_walk_in_pantry
,has_navigable_to_ocean
,has_one_plus_half_baths
,is_california_ranch
--,is_daily_rental_allowed
,is_no_lease_first_year
,is_ok_to_lease_first_year
,is_property_free_standing
--,is_public_land_bordered
--,is_tenant_approval
--,is_usda_qualified_zip
--,is_va_qualified_zip
,basement_perc_finished
,cdd_fee_amt_per_month
,mls_board_display
,sale_type
,property_disclaimer

from stage.direct_idx_attribute s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
where s.source_id in {} and t.target_listing_id is not NULL;
"""

LISTING_ATTRIBUTE_QUERY_2 = """
 select 
 t.source_id as source_id     ,
 t.batch_id as batch_id      ,
 t.target_listing_id  as listing_id    ,
 source_creation_date      ,
 source_last_update_date      ,
 t.y_creation_date      ,
 y_last_update_date   ,
 is_daily_rental_allowed ,
 is_tenant_approval
,has_waterfront_open_water
,is_shares_in_cooperative_ownership
,has_appliance_gas_stove
,is_pending_status
,is_active_under_contract_status
,is_rehab_financing
,baths_one_quarter
,baths_three_quarter
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(terms_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as terms_info
,is_condo_type_lower
,is_condo_type_other
,is_condo_type_upper
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(original_price_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as original_price_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(condo_fee_period_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as condo_fee_period_info
,hoa_two_fee_amt_per_month
,is_flood_zone_ae
,is_flood_zone_ve
,is_flood_zone_x
,is_flood_zone_cobra
,treb_is_sqft_1000_1199
,treb_is_sqft_1200_1399
,treb_is_sqft_1400_1599
,treb_is_sqft_1600_1799
,treb_is_sqft_1800_1999
,treb_is_sqft_2000_2249
,treb_is_sqft_2250_2499
,treb_is_sqft_2500_2749
,treb_is_sqft_2750_2999
,treb_is_sqft_3000_3249
,treb_is_sqft_3250_3499
,treb_is_sqft_3500_3749
,treb_is_sqft_3750_3999
,treb_is_sqft_4000_4249
,treb_is_sqft_4250_4499
,treb_is_sqft_4500_4749
,treb_is_sqft_500_599
,treb_is_sqft_600_699
,treb_is_sqft_700_799
,treb_is_sqft_800_899
,treb_is_sqft_900_999
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(room_type_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_type_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(room_description_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_description_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(room_length_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_length_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(room_width_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_width_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(room_level_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_level_info
,is_two_years_less
,is_rental_option
,is_investment_property
,is_style_single_wide
,total_num_of_baths
,has_water_source_hauled
,has_lower_floor_master
,has_owner_can_rent_option
,is_construction_off_frame_modular
,is_construction_on_frame_modular
,is_construction_site_built
,has_winter_view
,has_year_round_view
,has_long_range_view
,guest_house_sq_ft
,is_casita_style
,total_monthly_fees
,has_attached_apartment
,has_detached_apartment
,has_roof_deck
,has_storage
,is_pre_war
,is_walk_up
,strata_fee_amt
,has_community_exterior_maintenance
,has_community_lake
,has_community_road_maintenance
,has_community_ski_slope
,has_community_beach
,has_community_equestrian_area
,has_community_garbage_service
,has_community_power_boats_allowed
,has_community_public_safety
,is_lot_adjoins_game_land
,is_lot_adjoins_state_land
,is_lot_view_lot
,has_fireplace_insert
,has_radiant_floor
,has_wood_stove
,has_baseboard
,has_liquid_propane
,has_pellet_stove
,has_warm_air
,has_zoned_heat
,is_water_comm_central
,is_lot_waterfront_without_access_lot
,is_lot_waterfront_with_access_lot
,has_intracoastal_view
,has_appliance_dryer
,has_smooth_ceilings
,has_back_porch
,has_grill_built_in
,has_roof_asphalt
,has_roof_other
,is_construction_resale
,is_freehold_nonstrata
,is_freehold_strata
,has_first_floor_bathroom
,has_membership_no_equity_purchase_required
,is_construction_year_2010s
,is_construction_year_2000s
,is_construction_year_1940s
,is_new_construction_new_foundation
,is_construction_year_1990s
,is_construction_year_1900_to_1939
,is_construction_year_pre_1900
,is_new_construction_existing_foundation
,is_construction_year_1950s
,is_construction_year_1980s
,is_construction_year_1960s
,is_construction_year_1970s
,is_construction_age_100_plus_years
,is_construction_age_26_to_50_years
,is_construction_age_6_to_10_years
,is_construction_age_unknown
,is_construction_age_21_to_25_years
,is_construction_age_26_to_30_years
,is_construction_age_31_to_40_years
,is_construction_age_41_to_50_years
,is_construction_age_51_to_60_years
,is_construction_age_61_to_70_years
,is_construction_age_71_to_80_years
,is_construction_age_81_to_90_years
,is_construction_age_91_to_100_years
,is_construction_age_1_to_5_years
,is_construction_age_11_to_25_years
,is_construction_age_51_to_100_years
,is_new_construction_ready_for_occupancy
,is_recent_rehab
,is_construction_age_11_to_15_years
,is_construction_age_16_to_20_years
,is_rental_permitted
,has_propane_tank_leased
,has_propane_tank_owned
,has_plank_flooring
,is_not_age_restricted
,has_concrete_driveway
,is_underground_style
,is_construction_adobe
,is_construction_asbestos
,is_construction_asphalt
,is_construction_attic_crawl_hatchways_insulated
,is_construction_batts_insulation
,is_construction_block
,is_construction_blown_in_insulation
,is_construction_brick_veneer
,is_construction_cedar
,is_siding_cement
,is_construction_clapboard
,is_construction_concrete_composite
,is_construction_copper_plumbing
,is_construction_double_wall
,is_construction_drywall
,is_construction_ducts_professionally_air_sealed
,is_construction_exterior_duct_work_is_insulated
,is_construction_fiber_cement
,is_siding_fiber_glass
,is_construction_foam_insulation
,is_construction_glass
,is_construction_hardiplank_type
,is_construction_ICAT_recessed_lighting
,is_construction_ICFs
,is_siding_lap
,is_siding_log
,is_construction_low_VOC_insulation
,is_construction_manufactured
,is_construction_masonite
,is_construction_masonry
,is_siding_metal
,is_construction_natural_building
,is_construction_other
,is_construction_PEX_plumbing
,is_construction_plaster
,is_construction_pre_cast
,is_construction_radiant_barrier
,is_construction_rammed_earth
,is_construction_recycled_bio_based_insulation
,is_siding_redwood
,is_construction_rock
,is_siding_shake
,is_siding_shingle
,is_construction_single_wall
,is_construction_slump_block
,is_construction_spray_foam_insulation
,is_siding_steel
,is_construction_stone_veneer
,is_construction_straw
,is_construction_synthetic_stucco
,is_siding_t1_11
,is_construction_unknown
,is_siding_vertical
,is_siding_vinyl
,has_utilities_above_ground
,has_utilities_cable
,has_utilities_cable_connected
,has_utilities_cable_not_available
,has_utilities_electricity_connected
,has_utilities_electricity_not_available
,has_utilities_high_speed_internet_available
,has_utilities_high_speed_internet_connected
,has_utilities_municipal
,has_utilities_natural_gas_available
,has_utilities_natural_gas_connected
,has_utilities_natural_gas_not_available
,has_utilities_none
,has_utilities_other
,has_utilities_phone_available
,has_utilities_phone_connected
,has_utilities_phone_not_available
,has_utilities_propane
,has_utilities_septic_available
,has_utilities_sewer_available
,has_utilities_sewer_connected
,has_utilities_sewer_not_available
,has_utilities_underground
,has_utilities_water_available
,has_utilities_water_connected
,has_utilities_water_not_available
,is_furnished_or_unfurnished
,is_furniture_negotiable
,has_breed_restrictions
,has_pet_call_restrictions
,has_conditional_restrictions
,has_negotiable_restrictions
,has_no_dogs_restrictions
,has_owner_only_restrictions
,has_pet_restrictions
,is_siding_board_and_batten
,is_siding_composite
,is_lot_near_golf_course
,is_lot_near_ski_area
,is_lot_near_public_transport
,has_master_none
,has_master_combo_tub_and_shower
,has_master_separate_shower
,has_master_separate_tub
,has_master_spa_tub_and_shower
,has_master_whirlpool_spa
,has_master_dual_sinks
,has_master_bidet
,has_master_two_master_baths
,has_master_bedroom_sitting
,has_master_upstairs
,has_master_two_master_suites
,maintenance_free_includes_gas
,maintenance_free_includes_common_areas
,maintenance_free_includes_maintenance_exterior
,maintenance_free_includes_maintenance_interior
,maintenance_free_includes_lawn_care
,maintenance_free_includes_common_re_tax
,maintenance_free_includes_pest_control
,maintenance_free_includes_pool_service
,maintenance_free_includes_trash_removal
,maintenance_free_includes_parking
,maintenance_free_includes_security
,maintenance_free_includes_roof_maintenance
,maintenance_free_includes_manager
,maintenance_free_includes_insurance_bldg
,maintenance_free_includes_insurance_interior
,maintenance_free_includes_insurance_other
,maintenance_free_includes_ac_maintenance
,maintenance_free_includes_master_antenna_tv
,maintenance_free_includes_cable
,maintenance_free_includes_golf
,maintenance_free_includes_elevator
,maintenance_free_includes_laundry_facilities
,maintenance_free_includes_management_fees
,maintenance_free_includes_legal_accounting
,maintenance_free_includes_assessment_fee
,maintenance_free_includes_impact_fee
,maintenance_free_includes_electric
,maintenance_free_includes_janitor
,maintenance_free_includes_fidelity_bond
,maintenance_free_includes_hot_water
,maintenance_free_includes_recreational_facility
,maintenance_free_includes_reserve_funds
,maintenance_free_includes_sewer
,maintenance_free_includes_water
,maintenance_free_includes_water_treatment
,maintenance_free_includes_other
,maintenance_free_includes_none
,is_construction_mixed
,has_exterior_metal_frame
,is_construction_pre_fab
,is_construction_elevated
,is_construction_hollow_tile
,is_construction_piling
,is_siding_fiber_cement
,has_utilities_electric
,has_utilities_no_electric
,has_utilities_gas_bottle
,has_utilities_gas_natural
,has_utilities_oil
,has_utilities_no_telephone
,has_utilities_lake_worth_drain_dis
,unit_front_exposure_east
,unit_front_exposure_west
,unit_front_exposure_southwest
,unit_front_exposure_southeast
,unit_front_exposure_south
,unit_front_exposure_northwest
,unit_front_exposure_northeast
,unit_front_exposure_north
,has_no_aggressive_breeds
,no_dogs_allowed
,has_restrictions_buyer_approval
,has_restrictions_tenant_approval
,has_restrictions_interview_required
,is_lease_ok
,is_lease_ok_with_restrict
,is_no_lease_1st_year
,has_restrictions_none
,has_no_truck_rv_restrictions
,has_maximum_number_vehicles
,has_no_corporate_buyers
,is_daily_rentals_ok
,has_restrictions_other
,is_no_lease_first_2_years
,has_commercial_vehicles_prohibited
,has_historic_designation
,no_50_plus_lb_pets_allowed
,total_num_of_baths_ground_floor
,is_freehold_ownership
,is_condominium_ownership
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(condo_fee_includes_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as condo_fee_includes_info
,has_cooling_window_units
,has_ductless_HP_Mini_split_heat
,has_HEPA_air_filtration
,has_high_efficiency_source_heat
,has_hot_water_recirc_pump
,has_HRV_ERV_system
,has_insert_source_heat
,has_ninety_percent_high_efficiency_source_heat
,has_radiator_heat
,has_stove_free_standing_heat
,has_tankless_watery_heater
,has_wall_heat
,is_swim_tennis_community
,hoa_total_fee_amt_per_month
,has_elevator_none
,has_elevator_private
,has_elevator_secured
,has_rear_exposure_east
,has_rear_exposure_north
,has_rear_exposure_northeast
,has_rear_exposure_northwest
,has_rear_exposure_south
,has_rear_exposure_southeast
,has_rear_exposure_southwest
,has_rear_exposure_west
,has_hoa_amenities_snow_removal
,has_two_kitchens
,is_age_six_to_fifteen_years
,is_age_zero_to_five_years
,is_age_sixteen_to_thirty_years
,is_age_thirty_one_to_fifty_years
,is_age_one_hundred_plus_years
,is_age_fifty_one_to_ninety_nine_years
,is_home_type_allowed_approval_required
,is_home_type_allowed_manufactured
,is_home_type_allowed_mobile
,is_home_type_allowed_modular
,is_home_type_allowed_site_built
,is_not_twin_home_style
,has_no_restrictive_covenants
,is_not_manufactured_home_style
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(	dim_bedroom_1_info	,'None',''),', ',','),',,',',')),',')),''),',') as dim_bedroom_1_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(	dim_bedroom_2_info	,'None',''),', ',','),',,',',')),',')),''),',') as dim_bedroom_2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(	dim_bedroom_3_info	,'None',''),', ',','),',,',',')),',')),''),',') as dim_bedroom_3_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(	dim_bedroom_4_info	,'None',''),', ',','),',,',',')),',')),''),',') as dim_bedroom_4_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(	virtual_tour_2_info	,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as virtual_tour_2_info
,is_owner_occupied
,is_tenant_occupied
,is_vacant
,is_style_shot_gun
,has_dunes_view
,has_neighborhood_view
,has_parking_assigned
,lot_size_acres
,is_property_sub_type_not_manufactured
,has_mountain_ocean_view
,has_garden_view
,is_lease_hold_fa_ownership
,has_photovoltaic
,has_diamond_head_view
,has_sunrise_view
,has_sunset_view
,has_frontage_golf_course
,is_lot_agricultural_vine_vineyard
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(body_of_water_info,'None',''),', ',','),',,',',')),',')),''),',') as body_of_water_info
,has_paved_street
,has_public_street
,has_easement_street
,faces_east
,faces_north_east
,faces_north
,faces_north_west
,faces_south_east
,faces_south
,faces_south_west
,faces_west
,has_standard_rental_license_type
,has_airbnb_rental_license_type
,has_short_term_rental_license_type
,has_rental_license
,has_basement_dirt_floor
,has_basement_exterior_entry
,has_basement_separate_living
,has_basement_storage_space
,has_basement_other
,has_no_central_air_conditioning
,pets_allowed_with_restrictions
,has_garage_common
,is_style_permanent_double_wide
,has_fence_partial
,has_fence_privacy
,has_fence_perimeter
,has_garage_2_plus
,has_ada_access
,has_air_conditioning_split
,has_frontage_conservation
,has_frontage_lake_pond
,has_patio_deck
,has_pool_spa_hot_tub
,is_lot_park_nearby
,is_lot_horse_property_unimproved
,has_first_floor_3_4_bathroom
,stories_numeric
,is_not_senior_community_55_plus
,building_name
,complex_name
,is_not_short_term_rental_allowed
,other_fees
,is_other_fee_freq_monthly
,is_other_fee_freq_quarterly
,is_other_fee_freq_semi_annual
,is_other_fee_freq_annual
,total_spaces_garage_and_parking
,has_elevator_common
,bedrooms_down
,has_golf_cart_parking
,has_master_multiple_shower_heads
,has_master_bath_remarks
,has_guest_quarters_separate_entrance
,has_no_multiple_offers_received
,builder_architect_name
,is_governing_bodies_condo
,is_governing_bodies_coop
,is_governing_bodies_hoa
,is_governing_bodies_none
,has_carport_attached
,has_carport_deached
,has_common_garage
,has_individual_garage
,has_garage_attached_one_stall
,has_garage_attached_two_stalls
,has_garage_attached_three_stalls
,has_garage_attached_four_stalls
,has_garage_detached_one_stall
,has_garage_detached_two_stalls
,has_garage_detached_three_stalls
,has_garage_detached_four_stalls
,is_construction_bermed
,is_construction_timber_frame
,is_construction_steel_frame
,is_exterior_concrete_block
,is_construction_mobile
,is_construction_on_permanent_foundation
,is_construction_modular
,is_on_intracoastal_waterway_lot
,is_oceanfront_lot
,is_lake_pond_lot
,is_channel_lot
,is_inside_city_limits_lot
,is_outside_city_limits_lot
,is_island_lot
,is_ocean_view_lot
,is_second_row_beach_lot
,is_second_row_other_lot
,is_floating_dock_lot
,is_in_intracoastal_waterway_community_lot
,is_marsh_view_lot
,is_horse_allowed
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(covid_remarks,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',')as covid_remarks
,is_possession_at_close
,is_possession_close_plus_16_to_29_days
,is_possession_close_plus_one_day
,is_possession_close_plus_two_days
,is_possession_close_plus_thirty_days
,is_possession_close_plus_31_to_60_days
,is_possession_close_plus_3_to_5_days
,is_possession_close_plus_6_to_15_days
,is_possession_negotiable
,is_possession_other
,is_possession_rental_agreement
,is_possession_seller_rent_back
,is_possession_subject_to_tenant_rights
,has_second_floor_bdrm
,has_third_floor_bdrm
,has_fourth_floor_bdrm
,has_basement_bdrm
,has_lower_floor_bdrm
,has_upper_floor_bdrm
,is_land_type_commercial
,is_land_type_farm
,is_land_type_industrial
,is_land_type_ranch
,is_land_type_residential
,is_land_type_unimproved_land
,is_frontage_golf_course
,is_frontage_preserved_area
,is_active_with_offer
,has_restrictions_association_approval_required
,has_restrictions_renting_limited
,has_restrictions_corporate_buyer_ok
,has_restrictions_exterior_alterations
,has_restrictions_auto_parking_on
,has_restrictions_children_ok
,has_restrictions_min_down_payment_required
,has_restrictions_additional_restrictions
,has_restrictions_no_corporate_ownership_allowed
,lanai_sqft_info
,sqft_info
,has_parking_paved
,has_garage_door_opener
,has_golf_cart_garage
,has_attached_carports
,has_detached_carports
,has_handicap_parking
,has_parking_unpaved
,has_parking_guest
,has_parking_common
,has_waterfront_bay_access
,has_waterfront_beach_access
,has_waterfront_canal_access
,has_waterfront_river_access
,is_rectangular_lot
,is_zero_lot_line_lot
,is_cleared_lot
,is_lot_acreage
,is_multiple_lots_lot
,has_flood_plain_none
,has_flood_plain_house_in_flood
,has_flood_plain_100_yr
,has_flood_plain_500_yr
,has_flood_plain_lender_may_require_ins
,has_flood_plain_partial
,has_exterior_adj_to_blm
,has_exterior_recereational
,has_exterior_hunting
,has_exterior_adj_to_bureau_of_reclamation
,is_condition_tear_down
,is_active_contingent_status
,is_pending_taking_backups_status
,capital_improvement_fee
,is_zoning_family_one_half_to_two_acres
,is_zoning_family_two_acres_or_more
,is_zoning_family_less_than_half_acre
,is_zoning_light_industrial
,is_zoning_mobile_homes
,is_zoning_multi_family
,is_zoning_professional
,is_zoning_rural
,is_level_no_one_above_common_walls
,is_level_no_one_below_common_walls
,has_den_three_plus
,has_sewer_available
,has_sewer_not_available
,has_sewer_on_property
,has_sewer_connected
,is_condition_average
,is_condition_fair
,is_condition_good
,is_condition_restored
,is_condition_very_good
,has_utilities_telephone_connected
,has_utilities_telephone_available
,has_utilities_phone_on_property
,has_utilities_solar
,has_utilities_propane_needed
,has_utilities_internet_wifi
,has_utilities_google_fiber
,is_pool_screen_closure
,is_pool_outside_bath_access
,is_pool_cleaning_system
,is_pool_electric_heat
,is_pool_pool_equipment
,is_pool_other
,is_pool_gas_heat
,num_of_units_in_community
,has_irrigation_none
,has_irrigation_well
,has_irrigation_reclaimed_water
,has_irrigation_municipal
,has_irrigation_extra_cost
,has_irrigation_included_in_assessment
,is_gulf_access_bridges
,is_gulf_access_no_bridge
,is_gulf_access_no_bridge_water_direct
,is_gulf_access_other
,is_gulf_access_via_boat_lift
,is_gulf_access_via_boat_lock
,is_gulf_access_water_direct
,is_gulf_access_water_indirect
,is_frontage_type_bayharbor
,is_frontage_type_canal
,is_frontage_type_conservation
,is_frontage_type_golfcourse
,is_frontage_type_lagoonestuary
,is_frontage_type_lakefront
,is_frontage_type_marina
,is_frontage_type_oceanfront
,is_frontage_type_other
,is_frontage_type_parkgreenbelt
,is_frontage_type_preservation
,is_frontage_type_river
,is_frontage_type_see_remarks
,is_frontage_type_waterfront
,minimum_lease_days
,is_type_commercial_land
,is_lot_zero_lot_line
,has_gulf_view
,has_view_landscaped
,has_mangroves_view
,has_partly_buildings_view
,has_preserve_view
,total_num_of_beds_w_ensuite
,condo_fees_numeric
,is_fee_simple_townhouse
,is_condition_building_winterized
,is_condition_buildout_allowance
,is_condition_converted_use
,is_condition_decorator_allow
,is_condition_excellent
,is_condition_below_average
,is_condition_major_rehab_needed
,is_condition_needs_work
,is_condition_rehabilitation_potential
,is_condition_renov_remod
,is_condition_scope_project
,is_condition_shell
,is_condition_shows_well
,is_condition_will_do_buildout
,is_condition_average_plus
,is_condition_average_plus_prop
,has_clerestory_windows
,has_surround_sound_installed
,has_water_softener_loop
,has_water_softener_owned
,has_fence_rock_wall
,has_fence_pipe
,has_fence_adobe
,has_fence_block
,has_utilities_city_gas
,has_utilities_ebid
,has_utilities_el_paso_electric
,has_utilities_impact_fees_apply
,has_utilities_propane_butane
,has_community_water
,has_rv_access
,has_cooling_evaporative_window
,has_cooling_evaporative_central
,has_cooling_refrigerated_central
,has_cooling_refrigerated_window
,has_fireplace_gas
,has_hobby_room
,has_kitchen_gas_cooktop
,has_kitchen_electric_cooktop
,has_appliance_range_gas
,has_appliance_range_electric
,has_fence_dog_run
,has_awning
,has_exterior_stall_corrals
,is_style_solar
,is_style_southwestern
,has_roof_composition
,has_roof_flat
,has_roof_foam
,has_roof_gabled
,has_roof_hip
,has_roof_metal
,has_roof_tar_gravel
,has_roof_minimum_pitch
,has_roof_pitched
,is_house
,is_manufactured_double_wide
,is_two_third_family_style
,days_on_market
,is_philadelphia_style
,is_courtyard_style
,development_name_lov
,has_one_bedroom_plus_den
,has_two_bedrooms_plus_den
,has_water_rights
,has_no_contingency
,district_name
,has_common_dock
,has_patio_screened
,has_balcony_open
,has_porch_open
,has_roof_barrel
,lot_size_width
,has_pool_conventional
,has_water_type_bay
,has_water_type_brook
,has_water_type_cove
,has_water_type_harbor
,has_water_type_lake
,has_water_type_ocean
,has_water_type_pond
,has_water_type_river
,has_water_type_stream
,is_annual_occupied
,is_occupied_call_agent
,is_occupied_other
,has_road_frontage
,lot_depth
,project_name
,has_exterior_pier
,is_waterfront_directly_on_sand
,waterfront_amount
,waterfrontage_owned
,is_one_fourth_to_one_half_acre,
is_acre_condo_na,
is_one_to_two_and_half_acre,
is_one_half_to_one_acre,
is_25_to_50_acre,
is_acre_other,
is_2_6_to_5_acre,
is_10_1_to_25_acre,
is_5_1_to_10_acre,
has_waterfront_tidelands
,is_recreational_water_beach_rights
,is_recreational_water_boat_mooring
,is_recreational_water_boat_slip
,is_recreational_water_common
,is_recreational_water_deeded
,is_recreational_water_dock
,is_recreational_water_lake_freshwater
,is_recreational_water_nearby
,is_recreational_water_oceanfront
,is_recreational_water_public
,is_recreational_water_river_brook_stream
,is_recreational_water_row_to_water
,is_recreational_water_waterfront_deep
,is_recreational_water_waterfront_tidal
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(road_frontage_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as road_frontage_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(water_body_name,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as water_body_name
,has_extra_building
,is_frontage_island
,has_lower_level_laundry
,has_garage_main_level
,has_garage_downstairs
,general_property_taxes
,hoa_initiation_fee
,yearly_specials
,total_specials
,yearly_hoa_dues
,has_utilities_sewer_city
,has_utilities_water_city
,has_utilities_hauled_water
,has_utilities_irrigation
,has_utilities_needs_well
,has_utilities_private_sewer
,has_utilities_private_water
,has_utilities_wwt_sewer_septic_combo
,has_utilities_shared_well
,has_utilities_3_phase
,s.year_built
,has_appliance_hot_tap
,has_kitchen_warming_drawer
,has_wine_cooler
,is_land_restrict_mfg
,is_land_restrict_sb_or_mfg
,is_land_restrict_sb
,is_condo_five_plus_stories
,is_residential_annual
,is_condo_coop_off_season
,is_townhouse_fee_simple
,is_condo_coop_annual
,is_condo_coop_seasonal
,is_pool_only
,is_wf_pool_no_ocean_access
,is_condo_one_to_four_stories
,is_no_pool_no_water
,is_townhouse_villa_annual
,is_duplex_tri_quad_annual
,is_coop_one_to_four_stories
,is_townhouse_villa_seasonal
,is_apartments_annual
,is_residential_seasonal
,is_wf_ocean_access
,is_wf_pool_ocean_access
,is_wf_no_ocean_access
,is_villa_condo
,is_coop_five_plus_stories
,is_condo_timeshare
,is_duplex_tri_quad_seasonal
,is_villa_fee_simple
,is_residential_off_season
,is_apartments_seasonal
,is_style_efficiency
,is_eff_std_hotel_room_annual
,is_eff_std_hotel_room_off_season
,is_duplex_tri_quad_off_season
,is_townhouse_villa_off_season
,is_apartments_off_season
,has_boat_lock
,is_deeded_dock
,has_boat_hoist_davits
,has_wateraccess_none
,has_wateraccess_other
,has_wateraccess_restricted_saltwater_access
,has_wateraccess_unrestricted_saltwater_access
,is_pets_maximum_20_lbs
,is_pets_more_than_20_lbs
,is_pets_no_cats_allowed
,is_pets_no_dogs_allowed
,is_pets_no_aggressive_breeds
,is_hoa_period_na
,approx_heated_sq_ft
,has_bedrooms_3
,has_community_barbecue
,has_community_cable_tv
,has_community_meeting_room
,has_community_racquet_ball
,has_community_none
,has_community_other
,is_spanish_mediterranean
,is_style_cluster_home
,is_style_none
,is_not_condo_townhouse
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(pets_limit_breed_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as pets_limit_breed_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(pets_limit_max_weight_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as pets_limit_max_weight_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(pets_limit_other_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as pets_limit_other_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(pets_limit_max_number_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as pets_limit_max_number_info
,is_single_family_home
,is_type_other
,is_five_plus_stories
,is_style_corner_unit
,is_type_is_floating_home
,is_style_inside_unit
,is_style_laneway_house
,is_style_live_work_studio
,is_style_upper_unit
,has_separate_dining_room
,has_fireplace_1
,has_fireplace_2_or_more
,has_wood_floors
,is_not_historic_home
,is_seller_not_owner,
is_financing_none,
is_estate_sale,
is_entered_as_comparable_sale,
is_court_approval,
has_primary_living_area_master_bdrm,
has_non_primary_living_area_master_bdrm,
is_architecture_low_rise,
has_boat_access_captainswalk,
is_pool_concrete_gunite,
has_canal_width_1to30,
has_boat_access_concretedock,
has_high_impact_doors,
units_in_building,
is_architecture_high_rise,
has_boat_access_dockincluded,
has_boat_access_dockdeed,
has_boat_access_boat_canopy_cover,
has_boat_access_boathouse,
is_spa_inground,
has_parking_free_parking,
is_spa_concrete_gunite,
has_boat_access_docklease,
has_impact_glass,
has_parking_circular_drive,
is_spa_private,
has_boat_access_boat_dock_private,
is_architecture_mid_rise,
has_canal_width_150to200,
has_canal_width_200plus,
has_boat_access_boatlift,
has_pets_with_approval,
has_canal_width_121to150,
has_boat_access_boatslip,
has_canal_width_31to80,
is_hoa_voluntary,
has_canal_width_81to120,
has_parking_ev_charging,
has_lot_dead_end,
has_parking_permit_required,
is_architecture_manufactured,
has_view_trees_woods,
has_boat_access_boatramp,
is_hoa_mandatory
,is_residential_lot
,is_multifamily_5_plus
,has_community_sauna
,is_raised_beach_style
,has_waterfront_channel
,has_channel_view
,has_club_area_view
,has_ocean_direct_view
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_bedroom_1_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_bedroom_1_info          
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_bedroom_2_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_bedroom_2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_bedroom_3_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_bedroom_3_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_bedroom_4_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_bedroom_4_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_bedroom_5_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_bedroom_5_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_breakfast_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_breakfast_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_den_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_den_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_dining_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_dining_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_dormer_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_dormer_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_enclosed_porch_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_enclosed_porch_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_exercise_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_exercise_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_great_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_great_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_library_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_library_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_loft_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_loft_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_master_bedroom2_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_master_bedroom2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_media_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_media_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_office_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_office_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_recreation_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_recreation_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_sitting_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_sitting_room_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.room_sun_room_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as room_sun_room_info
,has_kitchen_gas_stub_for_range
,monthly_lease_fee
,lease_renegotiation_date
,lease_expiration_date
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.hoa_2_fee_includes,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as hoa_2_fee_includes
,Additional_Fee
,maintenance_common_fee
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(s.additional_fee_includes,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as additional_fee_includes
,is_green_certified
,is_terrain_hilly
,is_terrain_mountain
,is_terrain_steep_slope
,is_property_desc_adjacent_to_golf_course
,has_drip_irrigation_man_full
,has_drip_irrigation_man_part
,has_drip_irrigation_auto_full
,has_drip_irrigation_auto_part
,has_rainwater_collection
,has_greywater_collection
,is_pervious_paving
,is_lot_private
,has_red_rock_view
,is_no_pool_no_water_style
,is_pool_only_style
,is_waterfront_no_ocean_access_style
,is_waterfront_pool_no_ocean_access_style
,is_waterfront_ocean_access_style
,is_waterfront_pool_ocean_access_style
,is_type_condo_coop_villa_townhouse
,has_condition_additions_alterations
,has_condition_building_permit
,has_condition_repair_cosmetics
,has_condition_repair_major
,has_condition_termite_clearance
,has_boat_access_dockpurchase
,is_lot_regular
,is_spa_electric_heat
,is_spa_gas_heat
,is_spa_solar_heat
,is_spa_pool_combo
,is_waterfront_basin
,is_waterfront_intersecting_canal
,is_waterfront_mangrove
,is_senior_community_62_plus
,has_washer_hookup
,is_1st_floor_unit
,is_2nd_floor_plus_unit
,has_boat_ramp
,is_deeded_water_access
,has_finished_room_over_garage
,is_short_term_rental_not_allowed
,has_accessible_elevator_installed
,has_accessibility_none
,has_no_stairs_internal
,has_grab_bars_in_bathroom
,has_doors_36_plus
,has_no_stairs_external
,has_hallways_42_plus
,has_door_level_handles
,has_accessibility_other
,has_stair_lift
,has_wheelchair_ramp
,has_partially_wheelchair
,has_sight_impaired
,has_roll_in_shower
,has_reduced_weight_counters
,has_fully_wheelchair
,has_reduced_weight_cabinets
,has_lowered_switches_controls
,is_property_sub_type_hpr_attached
,is_property_sub_type_hpr_detached
,is_condo_type_garden
,is_condo_type_high_rise
,is_condo_type_townhouse
,is_exp_cape_style
,is_exp_ranch_style
,is_nantucket_style
,is_sqft_2000
,is_sqft_4000
,is_sqft_6000
,is_sqft_8000
,has_free_parking
,has_chain_of_lakes_frontage
,is_long_term_rental_allowed
,is_long_term_rental_not_allowed
,pets_allowed_with_limits
,pets_allowed_no_approval
,is_four_or_more_stories
,has_boat_access_hoist_davit
,has_sewer_asessment_paid
,has_sewer_assessment_unpaid
,has_water_assessment_paid
,has_water_assessment_unpaid
,has_water_solar_heat
,has_water_softener
,has_multiple_lots
,has_adu_detached
,has_adu_attached
,has_bath_off_master
,is_lot_conservation_area
,is_acreage
,is_lot
,is_platted
,is_unplatted
,has_parking_triple_garage
,has_parking_attached
,has_parking_double_garage_attached
,has_parking_double_garage_detached
,has_parking_heated_garage
,has_parking_rv_access_parking
,has_parking_single_garage_attached
,has_parking_single_garage_detached
,unexempt_taxes_amt
,has_waterfront_intracoastal_access
,has_waterfront_lake_privileges
,has_lot_back_lane
,has_lot_backs_to_green
,has_parking_alley_access
,is_under_one_fourth_acre
,is_one_to_five_acre
,is_five_to_ten_acre
,is_ten_to_twenty_acre
,is_twenty_to_fourthy_acre
,is_above_fourthy_acre
,is_acre_not_applicable
,has_additional_living_room
,has_ravine_view
,is_type_duplex
,is_type_triplex
,is_type_quadplex
,is_type_retail
,has_community_pier
,has_community_pool_indoor
,has_parking_3_or_more
,is_first_nations_lease
,is_leasehold_postpaid_nonstrata
,is_leasehold_postpaid_strata
,is_leasehold_prepaid_nonstrata
,is_leasehold_prepaid_strata
,is_title_to_land_other
,is_undivided_interest
,is_vacation_ownership
,has_foundation_block
,has_foundation_slab
,has_foundation_stone
,has_foundation_granite
,has_foundation_gravel
,has_foundation_other
,has_foundation_pillar
,has_foundation_concrete
,has_foundation_brick
,has_spa
,has_no_spa
,is_water_year_round_deep_water_access
,is_brow_lot
,is_water_year_round_lk_rvr
,is_water_summer_lk_rvr
,is_water_frontage_lk_rvr
,is_sole_proprietorship_ownership
,is_land_lease_ownership
,is_corporate_ownership
,is_franchise_ownership
,is_partnership_ownership
,is_reo_ownership
,is_contract_owner_ownership
,is_finance_co_ownership
,has_bunk_alcove
,has_bunk_room
,has_carbon_monoxide_detector
,is_condo_balc_grill_allowed
,is_cross_fenced
,has_fenced_storage
,has_guest_house
,has_internet
,has_outdoor_hot_tub
,has_outdoor_shower
,is_pool_screened
,has_side_porch
,has_wrap_porch
,has_sauna_steam_shower
,has_security_lighting
,has_property_stables
,has_storage_in
,has_storage_out
,has_window_treatment
,has_wrap_around_balcony
,has_condo_assigned_parking
,has_condo_covered_parking
,has_condo_deeded_parking
,has_condo_lot_parking
,has_double_carport
,has_double_garage
,has_parking_on_street
,parking_others_see_remarks
,has_parking_side_entrance
,has_natural_stone_flooring
,has_split_brick_flooring
,has_wood_flooring
,has_boat_dock
,is_boat_lot
,is_boat_covered
,is_boat_facilities_none
,is_other_boat_facilities
,has_power_available
,has_rental_slip
,is_1st_come_1st_served_slip
,has_dock_slip_deeded
,has_trailer_storage
,has_water_available
,is_less_4ft_water_depth
,is_4ft_plus_water_depth
,has_direct_bay_front_view
,has_direct_bay_across_road
,has_indirect_bay_side_view
,has_indirect_bay_across_road
,has_direct_bayou_front_view
,has_direct_bayou_across_road
,has_indirect_bayou_side_view
,has_indirect_bayou_across_road
,has_direct_gulf_front_view
,has_direct_gulf_across_road
,has_indirect_gulf_side_view
,has_indirect_gulf_across_road
,has_direct_ICW_front_view
,has_direct_ICW_across_road
,has_indirect_ICW_side_view
,has_indirect_ICW_across_road
,has_direct_lagoon_front_view
,has_direct_lagoon_across_road
,has_indirect_lagoon_side_view
,has_indirect_lagoon_across_road
,has_direct_lake_front_view
,has_direct_lake_across_road
,has_indirect_lake_side_view
,has_indirect_lake_across_road
,has_direct_river_front_view
,has_direct_river_across_road
,has_indirect_river_side_view
,has_indirect_river_across_road
,has_eastern_view
,has_northern_view
,has_skyline_view
,has_southern_view
,has_western_view
,is_bay_access_less_quarter_mile
,has_bay_front_building
,is_bayou_access_less_quarter_mile
,has_bayou_front_building
,is_beach_access_less_quarter_mile
,has_beach_side_building
,is_canal_access_less_quarter_mile
,has_canal_front_building
,is_creek_access_less_quarter_mile
,has_creek_front_building
,is_gulf_access_less_quarter_mile
,has_gulf_front_building
,is_ICW_access_less_quarter_mile
,has_ICW_front_building
,is_lagoon_access_less_quarter_mile
,has_lagoon_front_building
,is_lake_access_less_quarter_mile
,has_lake_front_building
,is_water_access_other_see_remarks
,is_public_access_less_quarter_mile
,is_river_access_less_quarter_mile
,has_river_front_building
,has_water_type_tier_1
,has_water_type_tier_2
,has_water_type_tier_3
,has_water_type_tier_4
,is_owner_association
,is_not_owner_association
,is_recurring_special_assessment
,is_not_recurring_special_assessment
,is_trust_ownership
,is_estate_ownership
,is_individual_ownership
,is_nj_lic_ownership
,has_irrigation_lake
,has_parking_two_spaces
,has_security_high_impact_doors
,has_spa_none
,is_florida_style
,leases_per_year
,has_east_facing_view
,has_north_facing_view
,has_west_facing_view
,is_style_not_neighbor_above
,is_style_not_neighbor_below
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(neighborhood_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as neighborhood_info
,floor_number
,has_shutters_electric
,has_shutters_manual
,has_spa_above_ground
,has_spa_fiberglass
,faces_direction_unknown
,is_spa_indoor
,is_tx_vet_financing
,has_community_basketball_court
,has_garage_oversized
,has_additional_dwelling
,has_lot_gutter
,has_first_floor_level_no_step
,has_interior_converted_garage
,has_eat_in_kitchen
,has_two_eating_areas
,has_secondary_bedroom_down
,room_desc_has_2_living_area
,room_desc_has_3_living_area
,is_sold_status
,has_other_bedroom_downstairs
,has_all_bedroom_downstairs
,has_all_bedroom_upstairs
,has_parking_one_assigned
,has_parking_two_assigned
,pet_limit_max_weight
,pet_limit_max_number
,has_gulf_access_none
,is_community_mobile_manufactured
,has_first_floor_owner_suite
,has_security_patrol
,has_tv_camera
,has_burglar_alarm
,has_gate_unmanned
,has_private_guard
,has_card_entry
,has_security_light
,has_no_security
,has_phone_entry
,has_lobby
,has_motion_detector
,has_security_system_owned
,has_security_system_leased
,has_security_wall
,has_key_card_entry_building
,has_complex_fenced
,has_fire_alarm
,has_garage_secured
,has_grillwork
,is_guard_at_site
,has_intercom_at_lobby
,has_lobby_secured
,has_lobby_attended
,has_no_burglar_alarm
,has_other_security
,has_key_card_entry_parking
,has_private_guards
,has_security_gate
,has_security_grill_work
,has_security_guard
,has_leased_burglar_alarm
,has_stairs_secured
,has_tv_monitor
,has_unit_alarm
,is_not_coop_ownership
,has_shared_backyard
,has_private_backyard
,has_private_roof_top
,has_public_roof_top
,is_lot_tank_pond
,is_lot_heavily_treed
,is_lot_has_come_trees
,is_lot_water_lake_view
,has_additional_level_laundry
,has_basement_level_laundry
,has_na_level_laundry
,is_multiplex_style
,is_not_type_duplex
,is_option_pending_status
,is_pending_continue_to_show_status
,has_restrictions_unknown
,has_waterfront_lake_main_body
,has_boat_dock_with_lift
,has_dock_covered
,has_dock_enclosed
,has_360_degree_view
,has_back_range_snow_capped_view
,has_foothills_view
,has_plains_view
,is_closed_status
,baths_three_quarter_main_level
,baths_full_main_level
,is_backsplit_style	
,is_sidesplit_style
,has_bamboo_floors
,has_cork_floors
,has_granite_floors
,has_linoleum_floors
,has_painted_stained_floors
,has_parquet_floors
,has_quartz_floors
,has_recycled_carpet_floors
,has_reclaimed_floors
,has_slate_floors
,has_simulated_wood_floors
,has_stamped_floors
,is_property_type_detached_with_com_elements
,is_property_type_mobile_trailer
,is_property_type_store_with_apt_office
,num_kitchens
,is_property_type_residential_commercial_mix
,is_property_type_multi_tenant_industrial
,is_property_type_house
,rental_monthly_income
,baths_above_grd
,is_property_country_homes_acreage
,total_num_of_beds_plus
,total_num_of_kitchens
,total_num_of_beds_main_level
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(development_name,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as development_name
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_actual_annual_vacancy_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_actual_annual_vacancy_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_advertising_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_advertising_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_cable_tv_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_cable_tv_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_electric_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_electric_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_elevator_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_elevator_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_furniture_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_furniture_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_gardener_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_gardener_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_gas_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_gas_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_insurance_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_insurance_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_licenses_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_licenses_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_maintenance_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_maintenance_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_maintenance_percent_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_maintenance_percent_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_management_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_management_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_manager_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_manager_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_operating_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_operating_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_operating_percent_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_operating_percent_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_other_description_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_other_description_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_other_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_other_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_pest_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_pest_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_pool_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_pool_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_security_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_security_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_supplies_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_supplies_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_tax_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_tax_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_total_annual_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_total_annual_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_trash_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_trash_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_water_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_water_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(expense_workers_compensation_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as expense_workers_compensation_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(model_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as model_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_active_lease_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_active_lease_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_beds_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_beds_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_full_baths_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_full_baths_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_furnished_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_furnished_13_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_half_baths_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_half_baths_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_lease_expires_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_lease_expires_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_monthly_rent_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_monthly_rent_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_sqft_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_sqft_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_01_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_01_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_02_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_02_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_03_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_03_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_04_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_04_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_05_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_05_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_06_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_06_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_07_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_07_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_08_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_08_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_09_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_09_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_10_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_10_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_11_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_11_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_12_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_12_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(unit_type_13_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as unit_type_13_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(water_island_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as water_island_info
,has_community_assisted_living_available
,has_community_barbeque
,has_community_basketball
,has_community_bbq_picnic
,has_community_beach_club_available
,has_community_beach_club_included
,has_community_beach_private
,has_community_beauty_salon
,has_community_bike_and_jog_path
,has_community_bike_storage
,has_community_billiard
,has_community_billiards
,has_community_boat_dock
,has_community_boat_lift
,has_community_boat_slip
,has_community_boat_storage
,has_community_bocce_court
,has_community_business_center
,has_community_cabana
,has_community_concierge_services
,has_community_electronic_vehicle_charging
,has_community_extra_storage
,has_community_fish_cleaning_station
,has_community_fishing_pier
,has_community_fitness_center_attended
,has_community_full_service_spa
,has_community_guest_room
,has_community_guest_suite
,has_community_gulf_boat_access
,has_community_hobby_room
,has_community_internet_access
,has_community_lakefront_beach
,has_community_landscaping
,has_community_library
,has_community_park
,has_community_pickleball
,has_community_pickle_ball
,has_community_picnic_area
,has_community_play_area
,has_community_private_beach_pavillion
,has_community_private_membership
,has_community_putting_green
,has_community_restaurant
,has_community_room
,has_community_rv_boat_storage
,has_community_shopping
,has_community_shuffleboard
,has_community_theater
,has_community_trails
,has_community_trash
,has_community_trash_chute
,has_community_underground_utility
,has_community_vehicle_wash_area
,has_community_volleyball
,has_community_water_skiing
,has_dog_park
,has_existing_lease
,has_fence_chain
,has_fence_other
,has_fence_wood
,has_fence_wrought
,has_garage_4_carport
,has_garage_5_carport
,has_garage_other
,has_laundry_hook_up_in_unit
,has_manufactured_flooring
,has_multiple_master
,has_no_community_amenities
,has_no_downstairs_bathroom
,has_no_downstairs_bedroom
,has_no_gas_heat
,has_other_flooring
,has_partial_view
,has_pellet_source_heat
,has_tankless_hot_water
,has_unit_level_master
,has_waterfront_across_street_from_ocean
,has_waterfront_bank_high
,has_waterfront_bank_low
,has_waterfront_bank_medium
,has_waterfront_beach_rights
,has_waterfront_bulkhead
,has_waterfront_gulf
,has_waterfront_jetty
,has_waterfront_no_bank
,is_assumable_financing
,is_condo_one_level
,is_condo_two_levels
,is_corporate_relocation_ownership
,is_exterior_brick
,is_fannie_mae_freddie_mac_ownership
,is_farm_home_loan
,is_ground_level_entry
,is_ground_level_with_steps_entry
,is_lower_level_with_elevator_entry
,is_lower_level_with_steps_entry
,is_manufactured_single_wide
,is_mid_level_with_elevator_entry
,is_mid_level_with_steps_entry
,is_one_and_one_half_story_w_basement
,is_one_story_w_basement
,is_parking_reserved
,is_planned_community
,is_pre_foreclosure
,is_property_sub_type_own_your_own
,is_rehab_loan
,is_rental_not_permitted
,is_sba_loan
,is_siding_other
,is_state_bond_loan
,is_two_stories_w_basement
,lot_size_sqft_living_range
,pets_allowed_with_breed_restrictions
,days_since_sold
,cap_rate
,hoa_fee_amt2_per_month
,land_payment
,num_acres
,total_num_of_beds_above_grade
,transfer_fee_amt
,has_master_other


from stage.direct_idx_attribute_2 s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
	
where s.source_id in {}  and t.target_listing_id is not NULL;
"""


LISTING_ATTRIBUTE_QUERY_3 = """
select 
 t.source_id as source_id     ,
 t.batch_id as batch_id      ,
 t.target_listing_id as listing_id     ,
 source_creation_date      ,
 source_last_update_date      ,
 t.y_creation_date      ,
 y_last_update_date   ,
 is_lot_ravine ,
is_beach_property,
has_contingency_with_offer,
has_contingency_not_with_offer,
is_style_triple_wide
,has_no_doorman	
,has_courtyard	    
,has_juliet_balcony
,is_property_unincorporated
,building_width
,is_post_war
,is_in_escrow_status
,is_parking_two_plus
,is_gulf_bay_view
,tax_amt_per_month
,has_garage_stall
,is_character_style
,is_parking_heated_garage
,is_style_westcoast
,has_parking_triple_garage_detached
,has_parking_insulated
,has_parking_shop
,has_parking_parkade
,has_parking_triple_garage_attached
,is_first_floor_condo			
,is_not_affordable_housing		
,has_basement_bedroom			
,has_basement_bathroom			
,is_affordable_housing			
,basement_lvl_bedrooms			
,basement_lvl_bathrooms
,is_mobile_home_allowed
,is_mobile_home_not_allowed
,total_num_cars
,is_style_condo_timeshare
,is_style_coop_one_to_four_stories
,is_style_coop_five_plus_stories
,is_style_pool_only
,is_style_wf_no_ocean_access
,is_style_wf_pool_no_ocean_access
,is_style_wf_ocean_access
,is_style_wf_pool_ocean_access
,is_villa_fee_simple_style
,is_villa_condo_style
,is_cluster_home_style
,has_four_bedrooms_plus_den
,has_five_bedrooms_plus_den
,has_heat_fan_coil
,has_other_heat_source
,has_electric_heat_source
,has_appliances_stainless_steel
,has_walk_out_to_yard
,has_crown_moulding
,has_bi_appliances
,has_pot_lights
,has_ensuite_bath
,space_rent_per_month
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(space_rent_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as space_rent_includes_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(basement_development_info,'None',''),', ',','),',,',',')),',')),''),',') as basement_development_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(basement_features_info,'None',''),', ',','),',,',',')),',')),''),',') as basement_features_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(equipment_type_info,'None',''),', ',','),',,',',')),',')),''),',') as equipment_type_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(exterior_finish_info,'None',''),', ',','),',,',',')),',')),''),',') as exterior_finish_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(fireplace_fuel_info,'None',''),', ',','),',,',',')),',')),''),',') as fireplace_fuel_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(fireplace_type_info,'None',''),', ',','),',,',',')),',')),''),',') as fireplace_type_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(foundation_type_info,'None',''),', ',','),',,',',')),',')),''),',') as foundation_type_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(land_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as land_amenities_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(landscape_features_info,'None',''),', ',','),',,',',')),',')),''),',') as landscape_features_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(maintenance_fee,'None',''),', ',','),',,',',')),',')),''),',') as maintenance_fee
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(maintenance_fee_period_info,'None',''),', ',','),',,',',')),',')),''),',') as maintenance_fee_period_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ownership_type_info,'None',''),', ',','),',,',',')),',')),''),',') as ownership_type_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(roof_style_info,'None',''),', ',','),',,',',')),',')),''),',') as roof_style_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(storage_type_info,'None',''),', ',','),',,',',')),',')),''),',') as storage_type_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(structure_info,'None',''),', ',','),',,',',')),',')),''),',') as structure_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(swimming_pool_features_info,'None',''),', ',','),',,',',')),',')),''),',') as swimming_pool_features_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(utility_power_info,'None',''),', ',','),',,',',')),',')),''),',') as utility_power_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(utility_sewer_info,'None',''),', ',','),',,',',')),',')),''),',') as utility_sewer_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(utility_water_info,'None',''),', ',','),',,',',')),',')),''),',') as utility_water_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(waterfront_name_info,'None',''),', ',','),',,',',')),',')),''),',') as waterfront_name_info
,has_updated_kitchen
,is_rentable_restrictions_may_apply
,is_lot_adjoins_golf_course
,has_has_community_maintenance_provided
,is_eichler_style
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(land_size_info,'None',''),', ',','),',,',',')),',')),''),',') as land_size_info
,rental_monthly_income_collected
,has_dock_in_place
,unit_rent_01
,unit_rent_02
,unit_rent_03
,unit_rent_04
,current_space_rent
,is_expired_pending_status
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(showing_expected_date_to_begin_info,'None',''),', ',','),',,',',')),',')),''),',') as showing_expected_date_to_begin_info
,land_terms
,is_type_land
,is_level_two_levels_or_one_and_one_half
,is_level_one_and_one_half
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(municipality_info,'None',''),', ',','),',,',',')),',')),''),',') as municipality_info
,is_rentable
,has_cryptocurrency_financing
,is_property_sub_type_five_or_more_units
,has_sewer_assessment_paid
,has_sewer_betterment
,has_water_reverse_osmosis_entire_house
,has_water_betterment
,has_water_dual
,has_water_filter
,has_water_none
,has_water_other
,has_water_reverse_osmosis_partial_house
,has_water_solar_heater
,has_flood_plain_restrictions
,is_lease_less_than_one_year
,is_rental_month_to_month
,has_guest_quarters
,is_type_hotel
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(unit_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as unit_amenities_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(balconyporchlanai_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as balconyporchlanai_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(basement_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as basement_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bathroom1_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bathroom1_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bathroom2_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bathroom2_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bathroom3_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bathroom3_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bathroom4_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bathroom4_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom1_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom1_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom2_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom2_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom3_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom3_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom4_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom4_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom5_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom5_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom6_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom6_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom7_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom7_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom8_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom8_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom9_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom9_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bonusroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as bonusroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(breezeway_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as breezeway_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(den_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as den_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(dinette_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as dinette_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(diningroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as diningroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(familyroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as familyroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(floridaroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as floridaroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(foyer_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as foyer_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(gameroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as gameroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(greatroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as greatroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(kitchen_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as kitchen_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(laundry_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as laundry_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(library_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as library_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(livingroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as livingroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(loft_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as loft_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(masterbathroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as masterbathroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(masterbedroom2_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as masterbedroom2_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(masterbedroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as masterbedroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mediaroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as mediaroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(office_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as office_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(sauna_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as sauna_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(studio_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as studio_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(utilityroom_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as utilityroom_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(workshop_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as workshop_room_dimension_info
,unit_1_beds
,unit_1_baths
,unit_2_beds
,unit_2_baths
,unit_3_beds
,unit_3_baths
,unit_4_beds
,unit_4_baths
,unit_total_beds
,unit_total_baths
,has_exposure_east_west
,has_mello_roos
,has_no_mello_roos
,is_proposed_construction_status
,has_pikes_peak_view
,has_suite_illegal
,has_suite_legal
,has_airport_runway
,has_association
,has_no_association
,is_type_office
,is_type_warehouse
,has_foundation_raised
,has_foundation_crawl_space
,is_lake_chain_eagle_river
,is_lake_chain_cisco
,is_lake_chain_three_lakes
,is_lake_chain_fence_lake_ldf
,is_lake_chain_minocqua
,is_lake_chain_natural_lakes
,is_lake_chain_turtle_winchester
,is_lake_chain_rhinelander
,is_lake_chain_presque_isle
,is_lake_chain_nokomis
,is_lake_chain_manitowish
,is_lake_chain_sugar_camp
,is_lake_chain_phillips
,is_lake_chain_big_saint_germain
,is_lake_chain_moen
,is_lake_chain_high_fishtrap
,is_lake_chain_shishebogama_gunlock
,is_lake_chain_pike_round
,is_lake_chain_sugarbush
,is_lake_chain_post_lake
,is_lake_chain_spider_lake_chain
,is_lake_chain_fisher
,is_lake_chain_wisconsin_river_system
,is_waterfront_deeded_access
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(lake_name_info,'None',''),', ',','),',,',',')),',')),''),',') as lake_name_info

,has_water_source_community_well
,has_appliance_stainless_steel
,has_community_riding_trails
,has_dock_subdivision_access
,has_appliance_wine_cooler
,has_water_source_county
,has_community_air_strip
,has_dock_assigned_slip
,is_partially_wooded_lot
,has_appliance_freezer
,has_water_view_none
,is_zoning_agricultural
,is_zoning_residential
,has_smoke_detector
,has_satellite_dish
,has_gigabit_fiber
,is_steep_lot
,has_fire_pit
,has_dsl
,total_num_of_rooms
,has_doorman_full_time
,has_bike_storage
,living_room_count
,is_not_horse_property
,is_not_roof_comp_shingle

,has_bedroom_level_1
,has_wateraccess_boatlock
,has_wateraccess_salt_water_restricted
,has_wateraccess_salt_water_unrestricted
,total_num_of_master_beds
,is_lot_airstrip


,has_no_guest_house
,is_seller_carry_back
,is_construction_status_not_applicable
,covered_parking_spaces
,has_enginered_wood_floors
,has_CRI_green_label_plus_certified_carpet_floors
,has_indoor_outdoor_floors
,has_mexican_tile_floors
,has_sustainable_floors
,is_construction_status_previously_own

,has_paved_road
,has_lawn_pump
,has_rain_gutter
,has_roof_dimensional_shg
,is_exterior_trim_vinyl


,is_siding_hardboard
,is_construction_rough_sawn
,is_construction_brick_partial
,is_manufactured_land_lease
,is_manufactured_land_owned
,has_no_assessment_fee
,community_has_barbecue
,community_has_business_center
,community_has_concierge
,community_has_fire_pit
,community_has_fitness_center
,community_has_guest_suites
,community_has_park
,community_has_parking
,has_community_amenities_none
,has_community_basketball_court
,has_community_jogging_path
,has_livestock_permitted
,hoa_fee_amt_per_month_info_text
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(hoa_fee_freq_info,'None',''),', ',','),',,',',')),',')),''),',') as hoa_fee_freq_info
,tax_amt_info_text
,condo_fees_info_text
,has_no_condo_fees
,has_horse_property_arena
,has_horse_property_auto_water
,has_horse_property_corral
,has_horse_property_mare_motel
,has_horse_property_round_pen
,has_horse_property_shed
,has_horse_property_tack_room
,has_horse_property_trail_access
,has_horse_property_turnout
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(additional_fee_frequency_info,'None',''),', ',','),',,',',')),',')),''),',') as additional_fee_frequency_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(additional_fee_info,'None',''),', ',','),',,',',')),',')),''),',') as additional_fee_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(hoa_fee_two_period_info,'None',''),', ',','),',,',',')),',')),''),',') as hoa_fee_two_period_info
,bedroom_desc_has_master_bed_1st_fl
,bedroom_desc_has_master_bed_2nd_fl
,bedroom_desc_has_master_bed_3rd_fl
,community_has_basketball_court
,community_has_boat_facilities
,community_has_dog_park
,community_has_equestrian_facilities
,community_has_jogging_path
,community_has_kitchen_facilities
,community_has_storage_facilities
,has_additional_fee
,has_central_water
,has_community_health_club
,has_community_maintenance_provided
,has_community_sundeck
,has_cooling_none
,has_exterior_brick_front
,has_heat_combination
,has_laundry_chute
,has_laundry_common_area
,has_laundry_electric_dryer_hookup
,has_laundry_gas_and_electric_dryer_hookup
,has_laundry_gas_dryer_hookup
,has_laundry_hook_in_unit
,has_laundry_in_carport
,has_laundry_in_closet
,has_laundry_individual_room
,has_laundry_in_garage
,has_laundry_in_kitchen
,has_laundry_none
,has_laundry_outside
,has_laundry_propane_dryer_hookup
,has_laundry_stackable
,has_laundry_washer_hookup
,has_wall_furnace
,has_wateraccess_boatlift
,has_water_source_not_hauled
,is_cash_only_financing
,is_cash_only_terms
,is_conventional_refinance_terms
,is_fha_va_approved_terms
,is_fractional_ownership
,is_installment_terms
,is_in_standard_listing
,is_in_trust_listing
,is_joint_venture_terms
,is_mobile_home_owned_lot_style
,is_mobile_home_rented_lot_style
,is_multi_level_split_style
,is_not_coming_soon
,is_no_terms
,is_not_sale_auction
,is_other_terms_terms
,is_owner_financing_less_20k_down_terms
,is_owner_hold_2nd_mortgage_terms
,is_owtb_wrap_terms
,is_park_model_style
,is_presently_leased_terms
,is_releases_terms
,is_secondary_financing_terms
,is_sell_complete_terms
,is_seller_will_pay_closing_costs_terms
,is_sell_sub_zoning_terms
,is_stacked_townhouse
,is_style_floating_home
,is_style_park_model
,is_style_quad_wide
,is_subordinate_terms
,frontage_feet
,frontage_length
,lake_size
,maintenance_repairs_fee
,parking_fees
,total_floors
,has_ravine
,is_in_escrow_not_showing_status
,total_num_garages
,is_not_property_desc_north_south_exposure
,is_not_property_desc_east_west_exposure
,is_zoning_industrial
,is_zoning_short_term_rental
,is_zoning_single_family
,has_ducts_heat


,akmls_access_type_airstrip
,akmls_access_type_dedicated_road
,akmls_access_type_dirt

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(broker_commission_info,'None',''),', ',','),',,',',')),',')),''),',') as broker_commission_info
,is_two_quarter_to_three_fourth_stories
,is_three_quarter_to_three_fourth_stories
,is_leed_certified
,cdd_fee_amt_per_year
,has_sewer_within_500_feet
,has_sewer_over_500_feet
,has_sewer_septic_needed
,has_parking_private
,has_parking_storage
,has_parking_unassigned
,has_parking_car_attached_4_plus
,has_parking_car_detatched_4_plus
,has_parking_public
,has_parking_car_detatched_3
,has_parking_off_street
,has_parking_garage
,has_parking_detatched
,has_parking_lot
,has_parking_car_attached_1
,has_parking_driveway
,has_parking_car_attached_3
,has_parking_car_attached_2
,has_parking_underground
,has_parking_covered
,has_parking_tandem
,has_parking_waitlist
,has_parking_shared_driveway
,has_parking_valet
,has_parking_car_detatched_2
,has_parking_off_site
,has_parking_car_detatched_1
,land_lease_amt
,land_lease_amt_freq
,is_daylight_single_level
,is_float_home
,is_manufactured_triple_wide
,is_shop_home
,is_single_level_with_bonus_room,


has_water_source_municipal_over_500_feet,
has_water_source_municipal_within_500_feet,
has_water_source_none,
has_water_source_other,
has_water_source_spring,
has_water_source_seasonal,
is_location_6th_floor_or_higher,
is_location_basement,
is_location_between_3rd_and_5th_floors,
is_location_center,
is_end_location,
is_front_location,
is_location_ground_floor,
is_location_municipal,
is_rear_location,
is_location_residential,
is_location_rural,
is_location_second_floor,
is_location_other,
is_location_industrial,
is_location_commercial,
is_location_subdivision,
is_contingent_buyer_sale_status,
is_contingent_back_up_offers,
waterfront_lake_acreage,

has_atrium,
has_attic_access_only,
has_attic_expandable,
has_attic_finished,
has_attic_floored,
has_attic_none,
has_attic_other,
has_attic_partially_finished,
has_attic_partially_floored,
has_attic_permanent_stairs,
has_attic_pull_down_stairs,
has_attic_radiant_barrier_decking,
has_attic_storage_only,
has_auxillary_kitchen,
has_cable_tv_available,
is_interior_converted_garage,
has_florida_room,
has_pull_down_storage,
room_desc_has_three_living_area,
room_desc_has_two_eating_areas,
room_desc_has_two_living_area,
has_master_bath_double_vanity,
has_master_bath_garden_tub,
has_master_bath_none_no_tub_or_shower,
has_master_bath_separate_vanity,
has_master_bath_shower_only,
has_master_bath_single_vanity,
has_master_bath_tub_only,
has_master_bath_tub_has_whirlpool,
has_master_bed_ceiling_fan,
has_master_bed_full_bath,
has_master_bed_half_bath,
has_master_bed_multi_closets,
has_master_bed_outside_access,
has_master_bed_split,
has_master_bed_walk_in_closet,
has_roof_built_up_gravel,
has_roof_clay,
has_roof_concrete,
has_roof_heavy_composition,
has_roof_slate,
has_roof_wood,
has_twelve_plus_attic_insulation,
has_thirteen_to_fifteen_seer_ax,
has_sixteen_plus_seer_ac,
has_ninety_percent_efficient_furnace,
has_cellulose_insulation,
has_dehumidifier,
has_foam_insulation,
has_high_efficiency_water_heater,
has_programmable_thermostat,
has_radiant_barrier,
has_recirculating_hot_water,
has_smart_electric_meter,
has_variable_speed_hvac,
has_wind_power,
has_propane_leased,
has_propane_owned,
has_drought_tolerant_plants,
has_ef_irrigation_control,
has_energy_recovery_ventilator,
has_enhanced_air_filtration,
has_geothermal_hvac,
has_low_flow_commode,
has_low_flow_fixture,
has_mechanical_fresh_air,
has_rain_water_catchment,
has_rain_freeze_censor,
has_solar_combo,
has_solar_electric_system,
has_solar_hot_water,
is_one_hundred_percent_financing,
is_second_seller_carry,
is_assumption_non_qualifying,
is_assumption_with_qualifying,
is_buydown_financing,
is_investors_ok,
is_financing_other,
is_release_req_financing,
is_seller_req_qualify,
is_va_substitution,
has_first_floor_level_no_steps,
has_all_bedrooms_upstairs,

is_basement_desc_bath_stubbed,
is_basement_desc_boat_door,
has_community_airport_runway,
has_bedroom_none,
has_oversized_master,
has_basement_driveway_access,
is_exterior_brick_front,
is_property_sub_type_mixed_use,
is_property_sub_type_garage,
is_property_sub_type_mobile_home,

is_parking_single_garage_attached,
is_parking_single_garage_detached,
is_parking_double_garage_attached,
is_parking_double_garage_detached,
is_parking_triple_garage_attached,
is_parking_triple_garage_detached,
is_parking_quad_or_more_attached,
is_parking_quad_or_more_detached,
is_not_seasonal_style,
has_separate_entrance,
has_no_separate_entrance,


string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(floorplan_image_info,'None',''),', ',','),',,',',')),',')),''),',') as floorplan_image_info,
has_arctic_entry,
has_community_none,
has_estate_trust_ownership,
has_garage_suite,
has_garden_suite,
has_judicial_sale_ownership,
has_main_level_bed_bath,
has_no_garage_suite,
has_no_garden_suite,
has_no_restrictions,
has_no_secondary_suite,
has_parking_double_garage_attached,
has_parking_double_garage_detached,
has_parking_parking_pad_cement_paved,
has_parking_quad_or_more_attached,
has_parking_quad_or_more_detached,
has_parking_single_garage_attached,
has_parking_single_garage_detached,
has_parking_stall,
has_restrictions,
has_secondary_suite,
has_wateraccess_beach,
is_apartment_high_rise_style,
is_apartment_low_rise_style,
is_commercial_fin_req,
is_down_payment_assist,
is_duplex_front_and_back_style,
is_duplex_side_by_side_style,
is_duplex_up_and_down_style,
is_exchange_financing,
is_hillside_bungalow_style,
is_multi_level_apartment_style,
is_not_permanent_affordable_housing,
is_pending_sb_status,
is_permanent_affordable_housing,
is_seller_will_subordinate,
is_single_level_apartment_style,
is_usda_rural_development,
is_vacant_lot,
number_of_pets_restriction,

is_tri_level_front_back,
is_four_level_front_back,
is_level_four_level,
is_level_five_plus_levels,
is_level_garden_level,

has_paved_access,
has_dirt_access,
is_time_share_type,
is_share_ownership_type,
is_boat_slip_type,
is_other_type,

is_one_to_two_acre,
is_fifteen_plus_acre,
is_two_to_five_acre,
is_five_to_fourteen_acre,
is_lot_ag_exempt,
is_lot_on_blanco_river,
is_lot_on_borders_state_park_game_ranch,
is_lot_on_canyon_lake,
is_lot_on_comal_river,
is_lot_county_view,
is_lot_creek_seasonal,
is_lot_gently_rolling,
is_lot_on_guadalupe_river,
is_lot_hunting_permitted,
is_lot_improved_water_front,
is_lot_on_lake_dunlap,
is_lot_on_lake_mcqueeney,
is_lot_on_lake_medina,
is_lot_on_lake_placid,
is_lot_on_lake_seguin,
is_lot_on_meadow_lake,
is_lot_on_greenbelt,
is_lot_on_pedernales_river,
is_lot_on_san_marcos_river,
is_lot_secluded,
is_lot_unimproved_water_front,
is_lot_water_access,
is_lot_xeriscaped,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(virtual_tour_3_info,'None',''),', ',','),',,',',')),',')),''),',') as virtual_tour_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(virtual_tour_4_info,'None',''),', ',','),',,',',')),',')),''),',') as virtual_tour_4_info,

has_garage_faces_front,
has_garage_faces_rear,
has_garage_faces_side,

has_utilities_electricity_not_connected,
has_utilities_electricity_available,

is_on_the_gulf_beach,

has_heat_portable,
has_heat_stove,
has_heat_unknown_btv,

is_lot_within_half_mile_to_water,
has_waterfront_boardwalk,
has_pool_community_or_private,
has_waterfront_association_access,
has_waterfront_shared_access,
has_waterfront_other,
has_waterfront_lake_superior,

is_type_5_plus_family,
is_manufactured_cross_mode,

has_dwelling_sep_detached_with_kitchen,

has_secondary_suite_permit_by_seller,
has_no_secondary_suite_permit_by_seller,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(seller_agent_compensation_info,'None',''),', ',','),',,',',')),',')),''),',') as  seller_agent_compensation_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(buyer_agent_compensation_info,'None',''),', ',','),',,',',')),',')),''),',') as  buyer_agent_compensation_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(other_agent_compensation_info,'None',''),', ',','),',,',',')),',')),''),',') as  other_agent_compensation_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(compensation_based_on_info,'None',''),', ',','),',,',',')),',')),''),',') as  compensation_based_on_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(sub_agency_offered,'None',''),', ',','),',,',',')),',')),''),',') as  sub_agency_offered,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(distance_to_electric_info,'None',''),', ',','),',,',',')),',')),''),',') as  distance_to_electric_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(distance_to_gas_info,'None',''),', ',','),',,',',')),',')),''),',') as  distance_to_gas_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(distance_to_phone_info,'None',''),', ',','),',,',',')),',')),''),',') as  distance_to_phone_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(distance_to_sewer_info,'None',''),', ',','),',,',',')),',')),''),',') as  distance_to_sewer_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(distance_to_water_info,'None',''),', ',','),',,',',')),',')),''),',') as distance_to_water_info,

has_master_bdrm_basement,
has_restrictions_architectural_review,
has_restrictions_livestock_restriction,
has_restrictions_manufactured_home_allowed,
has_restrictions_manufactured_home_not_allowed,
has_restrictions_modular_home_allowed,
has_restrictions_modular_home_not_allowed,
has_restrictions_no_representation,
has_restrictions_square_feet,
has_community_on_site_managment ,
has_community_pets_owners_only ,
has_community_pool_outdoor ,
is_builder_a_p_dodson,
is_builder_alvarez,
is_builder_level,
is_builder_bardwell,
is_builder_d_r_horton,
is_builder_dsld,
is_builder_willie_and_willie_home_builders,
is_builder_home_south_communities,
is_builder_brad_marcotte_construction ,

has_lake_name_simcoe,

is_mobile_double_wide_with_land,
is_mobile_single_wide_with_land,
is_resort_property,

has_lake_name_couchiching,
has_lake_name_muskoka,
has_lake_name_joseph,
has_lake_name_rosseau,
has_lake_name_dalrymple,
has_lake_name_balsam,
list_price,

is_no_price_new_construction,
has_outbuilding_air_park,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(capital_contribution_info,'None',''),', ',','),',,',',')),',')),''),',') as capital_contribution_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(storm_protection_info,'None',''),', ',','),',,',',')),',')),''),',') as storm_protection_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(window_treatment_info,'None',''),', ',','),',,',',')),',')),''),',') as window_treatment_info,
year_roof_installed,
is_new_status,
is_purchase_money_mtg_financing,
is_mshda_financing,
has_lake_name_adams_lake,
has_lake_name_aero_lake,
has_lake_name_aldrich_lake,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(leases_per_year_info,'None',''),', ',','),',,',',')),',')),''),',') as leases_per_year_info,

has_water_source_private,
has_water_source_association,
has_water_source_public,
has_water_source_water_charge_extra,
has_water_source_well,
has_bathroom_remodeled,
has_horse_property_bridle_path_access,
has_horse_property_commercial_breed,
has_horse_property_commercial_board,
has_horse_property_hot_walker,
has_horse_property_other,
has_horse_property_stall,
has_utilities_sw_gas,
has_single_garage,
has_workshop_garage,
unit_one_bedroom_count_info,
unit_two_bedroom_count_info,
unit_three_bedroom_count_info,
has_no_ensuite_bath,


is_manufactured_home,
is_one_br_plus_den,
is_two_br_plus_den,
is_three_br_plus_den,
is_four_br_plus_den,
is_five_br_plus_den,
is_not_sewer_septic,
has_assignment_of_contract_status,
has_known_damage_status,
has_land_value_status,
has_accessibility_single_level_living,
has_active_permit,
has_expired_permit,
has_applied_for_permit,
has_no_permit,
has_other_permit,
is_lot_no_outlet,
has_double_width_or_more_driveway,
has_finished_driveway,
has_gravel_driveway,
has_rear_driveway,
has_side_driveway,
is_raised_ranch_with_bonus_room_style,
has_double_vanity,
has_separate_shower,
is_homeplex_style,
has_water_source_cistern,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Closed_Date_Info,'None',''),', ',','),',,',',')),',')),''),',') as Closed_Date_Info,
total_num_outbuildings,
is_one_bedroom,
is_two_bedroom,
is_three_bedroom,
is_four_bedroom,
is_five_bedroom,
is_land_type_improved_land,
has_water_included,
has_community_lawn_maint_inc,
has_community_bus_line,
has_community_central_tv_antenna,
is_exterior_masonite,
has_foundation_elv_construction,
is_style_interval,
is_ten_acre_plus,
has_lot_lakefront_moultrie,
has_lot_lakefront_marion,
has_lot_on_tennis_court,
has_lot_river_access,
has_lot_beachfront,
has_lot_lagoon,
has_lot_other,
has_dock_existing,
has_fence_metal_enclosed,
has_fence_iron,
has_fence_brick,
has_fence_vinyl,
has_elevator_shaft,
is_pool_elevated,
has_full_front_porch,
has_exterior_some_storm_wnd_doors,
has_exterior_special_yard_lights,
has_exterior_sea_wall,
has_exterior_thermal_windows_doors,
has_exterior_stoop,
has_exterior_other,
is_boat_slip_style,
is_condo_regime_style,
is_duplex_one_unit_style,
is_triplex_one_unit_style,
is_10_yr_warranty,
has_homeowners_prot_plan,
is_right_of_first_refusal,
has_lot_tidal_creek,
is_latent_defect,
is_not_construction_status_to_be_built,
is_not_construction_status_under_construction,
is_not_construction_resale,
has_no_assignment_of_contract_status,
has_no_known_damage_status,
has_no_land_value_status,
has_waterfront_ocean_highway,
has_water_source_domestic_well,
is_basement_ranch,
community_has_easement_lake,
has_view_forest,
has_view_san_francisco_peaks,
is_sta_fe_pueblo_style,


is_not_in_probate,
is_not_cash_financing,
is_estate_owned,
has_boat_access_composite_dock,
has_boat_access_elec_avail,
has_boat_access_jet_ski_lift,
has_boat_access_none,
has_boat_access_tiki_hut,
has_boat_access_water_avail,
has_boat_access_wooden_dock,
has_accessibility_feature_main_level_entry,
has_accessibility_feature_zero_grade_entry,
has_accessibility_feature_wide_doorways,
has_accessibility_feature_level,
has_accessibility_feature_handicap_convertible,
has_accessibility_feature_other_bath_modification,
has_accessibility_feature_smart_technology,
has_accessibility_feature_kitchen_modification,
has_no_bonus_game_room,
pets_small_under_25_lbs,
pets_large_over_25_lbs,
is_pets_allowed_negotiable,
is_single_family_ownership,
has_restriction_architectural,
has_restriction_endangered_species,
has_restriction_limited_build_time,
has_restriction_limited_number_vehicles,
has_restriction_no_motorcycles,
is_not_subdivision_the_seabrook,
exterior_stories,
has_municipal_utility_district,
has_aerobic_septic,
has_utilities_alley,
has_utilities_all_weather_road,
has_utilities_asphalt,
has_utilities_coop_membership_included,
has_utilities_community_mailbox,
has_utilities_concrete,
has_utilities_coop_electricity,
has_utilities_coop_water,
has_utilities_curbs,
has_utilities_dirt,
has_utilities_gravel_rock,
has_individual_gas_meter,
has_individual_water_meter,
has_master_gas_meter,
has_master_water_meter,
has_utilities_no_city_services,
has_utilities_no_water,
has_utilities_overhead_utilities,
has_rural_water_district,
has_utilities_sidewalk,
has_utilities_sewer_tap_fee_paid,
has_utilities_unincorporated,
is_frog_attached,
is_lot_line_distance_to_electric,
is_lot_line_distance_to_gas,
is_lot_line_distance_to_phone,
is_lot_line_distance_to_water,
is_lot_line_distance_to_sewer,
num_of_residences,

has_no_sewer_cesspool,
is_style_not_mobile_home,


has_boat_launch,
has_bulkhead,
has_courtesy,
is_boat_covered_1_slip,
is_boat_covered_2_slip,
is_boat_covered_3_slip,
has_double_boat_slip,
has_dock_lights,
has_dock_party,
has_dock_platform,
is_zoning_horse_permitted,
 is_not_time_share,
 is_not_fractional_ownership,
has_no_irrigation_water_rights,
has_irrigation_water_rights,
is_present_use_yearly,
is_present_use_seasonal,
 is_multifamily_10_plus,
 is_zoning_mixed,
 is_zoning_pud,
 is_zoning_special_purpose,
 is_zoning_recreation,
 is_zoning_see_remarks,
is_flood_zone_unknown,
has_office_main,
 price_per_acre,
 has_shutters_none,
 has_shutters_screens_fabric,
is_type_full_duplex,
has_no_neighborhood_view,
has_no_no_view,
 has_sewer_septic_connected,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(compensation_comments_info ,'None',''),', ',','),',,',',')),',')),''),',') as compensation_comments_info

, adu_baths_total
, adu_beds_total
, is_not_ccr_lot
, is_type_fourplex
, is_old_spanish_style
, is_not_proposed_construction_status

,	has_hallway_laundry
,	has_downstairs_laundry
,	has_fiber_internet
,	has_walk_in_shower
,	has_cooling_swamp_cooler
,	is_exterior_not_brick_3_side
,	is_exterior_not_brick_4_side
,	is_income_producing
,	is_borders_special_land
,	is_controlled_access
,	is_not_fixer_upper
,	is_owner_may_carry_terms
,	is_barndominium_style
,	has_parking_rv_possible
,	has_parking_rv_garage_attached
,	has_rv_garage_detached
,	has_parking_rv_storage
,	has_boat_storage
,	has_water_source_community_water
,	has_water_source_tap_fee_req
,	has_water_source_aerator
,	has_water_sub_pump
,	has_sewer_lift_pump
,	has_water_mill_creek
,	has_sewer_not_connected
,	has_water_sump_pump
,	has_water_not_connected
,	has_water_source_ejector_pump
,	has_water_source_lagoon
,	has_water_source_no_sewer
,	has_waterfront_association_optional
,	has_waterfront_association_required
,	has_waterfront_brook
,	has_waterfront_seasonal
,	has_water_view
,	is_waterfront_walk_to
,	is_waterfront_not_direct
,	has_pool_alarm
,	is_pool_infinity_edge
,	has_pool_power_lift
,	has_pool_ramp_entrance
,	has_pool_slide
,	is_pool_tile
,	has_community_medical_facilities
,	has_community_paddle_tennis
,	has_community_private_rec_facilities
,	has_community_shuttle_service
,	is_pending_no_show_status
,	has_parking_rv_hook_ups
,	has_parking_rv_covered
,	has_parking_rv_paved
,	is_not_planned_community
,	has_in_suite_laundry
,	is_geodesic_style
,	is_deck_house_style
,	is_williamsburg_style
,	is_charleston_style
,	has_flood_irrigation
,	has_suite_no
,	has_suite_none
,	has_suite
, has_waterfront_block
, has_waterfront_natural
, has_boat_access_boathouse_w_utilities
, has_waterfront_sawgrass
, has_boat_slip_w_utilities
,	has_addl_living_space_family_room
,	has_addl_living_space_mud_room
,	has_addl_living_space_workshop
,	has_addl_living_space_addl_living_quarters
,	has_addl_living_space_apartment
,	has_addl_living_space_basement_apartment
,	has_addl_living_space_mother_in_law
,	has_addl_living_space_adu
,	has_addl_living_space_bonus_room
,	has_addl_living_space_den_study_office
,	has_addl_living_space_gameroom
,	has_addl_living_space_home_theater
,	has_addl_living_space_casita
,	has_addl_living_space_second_master_suit
,	has_addl_living_space_loft
,	is_lot_street_level
,	is_rolling_sloped_lot
,	has_accessibility_elevator
,	has_accessibility_accessible_approach_with_ramp
,	has_accessibility_wide_doors
,	has_accessibility_bath_rails
,has_no_carpet_floors  
,has_sprinkler_irrigation
,pool_type_private_info
,vegetation_info
,structure_type_info
,waterfront_features_info
,accessibility_features_info
,pool_spa_info
,entry_level_info
,has_no_oil_source_heat
,has_garage_extended
,has_water_source_private_company
,is_style_multi_wide


,has_bathroom_in_garage
,has_double_rv_garage
,has_garage_11_15_spaces
,has_garage_16_20_spaces
,has_garage_1_5_spaces
,has_garage_21_30_spaces
,has_garage_31_50_spaces
,has_garage_50_plus_spaces
,has_garage_6_10_spaces
,has_garage_air_conditioned
,has_garage_below_grade
,has_garage_community_drive
,has_garage_detached_garage
,has_garage_door_10_ft_height
,has_garage_door_11_ft_height
,has_garage_door_12_ft_height
,has_garage_door_13_ft
,has_garage_door_13_ft_height
,has_garage_door_14_plus_ft_height
,has_garage_door_7_ft_height
,has_garage_door_8_ft_height
,has_garage_door_9_ft_height
,has_garage_drive_through
,has_garage_epoxy_floor
,has_garage_finished
,has_garage_off_site
,has_garage_on_street
,has_garage_parking_in_common
,has_garage_private_drive
,has_garage_side_parking_8_10_ft_plus
,has_garage_side_parking_8_10_ft_plus_wide
,has_motor_home_garage
-------------------------------------------

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(masterbedroom_room_level_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as masterbedroom_room_level_info
,is_condition_poor 
,has_shared_driveway 
,has_private_single_driveway 
,has_driveway_open
,has_foundation_pier_and_beam 
,has_foundation_2_story 
,has_foundation_tri_level 
,has_foundation_combination 
,has_foundation_combo_beam 
,is_not_inside_subdivision 
,has_no_lagoon_view 
,has_no_lake_view 
,has_patio_deck_none 
,has_patio_deck_yes 
,has_patio_deck_uncovered 
,has_patio_deck_covered 
,has_patio_deck_enclosed_screen 
,has_patio_deck_enclosed_glass 
,has_patio_deck_breeze_open 
,has_patio_deck_breeze_closed
,pets_allowed_owner
,pets_allowed_tenant
,has_luxury_vinyl_floors
,has_no_covenants
,is_pool_solar_heat
,has_pool_sweep
,has_river_irrigation	
,is_cash_to_new_loan
,is_might_not_finance
,is_property_sub_type_multi_family
,is_terms_other
,is_lease_purchase
,is_submit_to_omc
,is_corp_approval_required
,is_property_sub_type_residential
,is_court_approval_required
,has_grandfather_rights_irrigation
,has_master_bath_no_tub
,has_garage_6_cars
,has_building_restrictions
,has_no_building_restrictions
,has_outbuilding_second_garage
,case when pets_max_number_info is not null then array[nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(pets_max_number_info,'None',''),', ',','),',,',',')),',')),'')::numeric::int] else null end as pets_max_number_info
,case when pets_max_weight_info is not null then array[nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(pets_max_weight_info,'None',''),', ',','),',,',',')),',')),'')::numeric::int] else null end as pets_max_weight_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(pets_deposit_fee_info,'None',''),', ',','),',,',',')),',')),''),',') as pets_deposit_fee_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(pets_deposit_description_info,'None',''),', ',','),',,',',')),',')),''),',') as pets_deposit_description_info
,is_agent_owned
,has_garage_attached_rear
,has_garage_attached_side
,has_garage_attached_detached
,is_not_leasehold_ownership
,is_not_freehold_ownership
,is_leasehold_ownership
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom1_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom1_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom2_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom2_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom3_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom3_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom4_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom4_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bedroom5_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bedroom5_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bathroom1_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bathroom1_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bathroom2_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bathroom2_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(room_recreation_level_info,'None',''),', ',','),',,',',')),',')),''),',') as room_recreation_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(diningroom_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as diningroom_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(kitchen_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as kitchen_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(livingroom_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as livingroom_room_level_info
,is_flood_zone_x500
,is_pets_deposit
,is_pets_monthly_fee
,is_near_public_transit
,has_parking_hangar
,is_construction_ground_level
,is_construction_partially_elevated
,is_construction_piling_concrete
,is_construction_piling_wood
,rent_type_annual
,rent_type_daily
,rent_type_monthly
,rent_type_weekly
,above_grade_sqft_range
,below_grade_sqft_range
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(basement_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as basement_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(bonus_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as bonus_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(laundry_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as laundry_room_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(storage_room_dimension_info,'None',''),', ',','),',,',',')),',')),''),',') as storage_room_dimension_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(storage_room_level_info,'None',''),', ',','),',,',',')),',')),''),',') as storage_room_level_info
,is_optional_furnished
,is_turnkey_furnished
,has_water_rural
,is_hoa_type_optional
,is_top_floor
,has_accessory_apartment
,has_in_law_suites_capability
,has_owner_suite_main
,has_split_floor_plan
,has_community_fishing
,has_clear_view
,has_ridge_view
,has_downtown_view
,has_garage_open
,building_age
,flr_area_fin_main_flr
,kitchens_plus
,has_waterfront_permit_required
,has_waterfront_stairway
,has_waterfront_waterfront_deck
,has_waterfront_lake_river_across_rd
,has_waterfront_seasonal_access
,has_waterfront_shared
,is_multifamily_2_to_4

,is_property_sub_type_offsite_built
,is_property_sub_type_onsite_built
,is_not_sloped_lot
,is_not_lot_level
,total_num_of_beds_ground_floor
,is_not_lot_rolling
,is_not_wooded_lot
,is_lot_mountainous
,is_not_lot_mountainous
,is_not_lot_secluded
,is_not_steep_lot
,has_ceilings_above_9_ft
,has_gym
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(maintenance_common_fee_info,'None',''),', ',','),',,',',')),',')),''),',') as maintenance_common_fee_info
,has_handicap_ramp
,has_other_water
,is_frontage_view_cove
,is_frontage_waterfront_community
,has_basement_master_bdrm
,has_handicap_bathroom
,has_handicap_doorway_min_36_in
,has_handicap_kitchen
,has_lake_drawn_water
,has_sewer_community
,has_sewer_lett_system
,has_sewer_septic_public_available
,has_well_public_available_water
,is_mobile_home_detitled
,is_mobile_home_not_detitled
,is_exterior_stucco_hard_coat
,is_exterior_wood_fiber_masonite
,is_frontage_common_lake
,is_frontage_common_pond
,is_frontage_deeded_lake_access
,is_frontage_on_lake_monticello
,is_frontage_on_lake_murray
,is_frontage_on_lake_wateree
,is_frontage_view_big_water
,is_shouse_style 
,is_style_tuck_under 
,is_oven_electric_range
,has_community_sewer_avail
,has_electric_less_than_1000_from_pl
,has_electric_less_than_100_from_pl
,has_electric_less_than_1_mile_from_pl
,has_electric_less_than_500_from_pl
,has_electric_more_than_1_mile_from_pl
,has_electric_none
,has_electric_yes_on_property
,has_private_water_avail
,has_sewer_city_avail
,has_utilities_underground_electric
,has_water_need_well
,has_water_source_city_avail
,has_water_source_city_on_prop
,is_not_fha_qualified
,is_pool_view_unit
,is_top_floor_unit
,is_water_view_unit
,has_carpet_pine_under_floors
,has_pine_floors
,has_screened_lanai
,has_boat_dry_storage
,has_electric_at_dock
,has_water_at_dock
,assessment_fee_amt_per_month
,is_2nd_floor_unit
,is_3rd_floor_unit
,is_4th_floor_unit
,is_5th_floor_unit
,is_6th_floor_unit
,is_7th_floor_unit
,is_8th_floor_unit
,is_9th_floor_unit
,is_10th_floor_unit
,is_11th_floor_unit
,is_12th_floor_unit
,is_13th_floor_unit
,is_14th_floor_unit
,is_15th_floor_unit
,is_timeshare_ownership
,is_cpr_ownership
,is_flood_zone_A
,is_flood_zone_AH
,is_flood_zone_AO
,is_flood_zone_D
,is_flood_zone_V
,has_basement_michigan
,has_pool_kidney_shaped
,has_pool_diving_board
,is_pool_on_ground
,has_community_boat_house
,is_waterfront_boat_slip_deed
,is_waterfront_boat_slip_off_site
,is_waterfront_boat_slip_lease_license
,is_waterfront_boat_slip_lease_license_offsite
,is_waterfront_boat_slip_community
,is_waterfront_covered_structure
,lot_creek_stream
,is_waterfront_none
,is_waterfront_paddlesport_launch_site
,is_waterfront_paddlesport_launch_site_community
,is_waterfront_personal_watercraft_lift
,is_waterfront_pier
,is_private_pond
,is_waterfront_retaining_wall
, has_pool_custom
, is_pool_geo_heat
, is_pool_bath
, is_pool_self_cleaning
,has_no_bedrooms
,is_not_electric_available
,is_arm_financing
,is_construction_loan_financing
,is_contract_financing
,is_development_financing
,is_fhma_financing
,is_hula_mae_financing
,is_open_financing
,is_other_financing
,has_parklike_view
,is_not_colonial_style
,has_parking_two_stalls
,has_garage_shop
,has_pool_on_ground
,has_waterfront_nearby
,is_waterfront_restricted_access
,is_waterfront_road_between
,has_basement_interior_entry
,has_lake_name_lake_cecebe
,has_lake_name_ahmic_lake
,has_lake_name_georgian_bay
,has_lake_name_six_mile_lake
,has_lake_name_gloucester_pool
,has_lake_name_little_lake
,water_control_depth
,has_mountain_front
,has_appliance_gas_water_heater
,has_alarm
,is_lot_square
,is_lot_reverse_pie_shaped_lot
,is_zoning_office
,is_rental_vacation
,tarmls_sub_restrict_gvr_available
,tarmls_sub_restrict_gvr_unavailable
,tarmls_sub_restrict_gvr_yes
,rapb_subdivision_amenities_street_lights
,rapb_subdivision_amenities_whirlpool
,rapb_subdivision_amenities_pool
,rapb_subdivision_amenities_golf_course
,rapb_subdivision_amenities_boating
,rapb_subdivision_amenities_tennis
,rapb_subdivision_amenities_bike_jog
,rapb_subdivision_amenities_horse_trails
,rapb_subdivision_amenities_clubhouse
,rapb_subdivision_amenities_basketball
,rapb_subdivision_amenities_elevator
,rapb_subdivision_amenities_lobby
,rapb_subdivision_amenities_fitness_center
,rapb_subdivision_amenities_extra_storage
,rapb_subdivision_amenities_common_laundry
,rapb_subdivision_amenities_community_room
,rapb_subdivision_amenities_game_room
,rapb_subdivision_amenities_library
,rapb_subdivision_amenities_sauna
,rapb_subdivision_amenities_shuffleboard
,rapb_subdivision_amenities_spa_hot_tub
,rapb_subdivision_amenities_trash_chute
,rapb_subdivision_amenities_picnic_area
,rapb_subdivision_amenities_sidewalks
,rapb_subdivision_amenities_bike_storage
,rapb_subdivision_amenities_billiards
,rapb_subdivision_amenities_business_center
,rapb_subdivision_amenities_cabana
,rapb_subdivision_amenities_courtesy_bus
,rapb_subdivision_amenities_beach_club_available
,rapb_subdivision_amenities_private_beach_pvln
,rapb_subdivision_amenities_manager_on_site
,rapb_subdivision_amenities_putting_green
,rapb_subdivision_amenities_workshop
,rapb_subdivision_amenities_horses_permitted
,rapb_subdivision_amenities_none
,rapb_subdivision_amenities_pilot_house
,rapb_subdivision_amenities_beach_access_by_easement
,rapb_subdivision_amenities_internet_included
,rapb_subdivision_amenities_cafe_restaurant
,rapb_subdivision_amenities_pickleball
,rapb_subdivision_amenities_bocce_ball
,rapb_subdivision_amenities_indoor_pool
,rapb_subdivision_amenities_park
,rapb_subdivision_amenities_playground
,rapb_subdivision_amenities_ball_field
,rapb_subdivision_amenities_soccer_field
,rapb_subdivision_amenities_dog_park
,rapb_subdivision_amenities_fitness_trail
,rapb_subdivision_amenities_runway_paved
,rapb_subdivision_amenities_runway_unpaved
,rapb_subdivision_amenities_airpark
,is_any_broker_advertise
,adu_total_num
,total_num_of_baths_main_level
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(community_name_info,'None',''),', ',','),',,',',')),',')),''),',') as community_name_info
,is_not_repairs_cosmetic
,is_not_repairs_major
,total_num_of_beds_below_grade
,has_wooded_view
,has_view_none
,has_residential_view
,has_rural_view
,has_community_rv_parking
,is_equestrian_community
,has_garage_2_plus_bays
,has_garage_3_plus_bays
,is_rental_allowed
,is_owner_occupant
,is_tenant_occupant
,is_vacant_occupant
,has_designated_builder
,has_no_designated_builder
,is_not_type_commercial_land
,is_mortgage_co_owned
,is_short_sale_potential
,is_veterans_affairs
,is_no_lease
,has_community_athletic_field
,has_community_landscape_maintenance
,has_community_lawn_maintenance
,office_level
,is_type_recreational
,has_view_vincent_thomas_bridge
,is_manufactured_hud
,is_manufactured_mobile_pre_june_1976
,is_manufactured_with_land
,is_manufactured_in_a_park_without_land
,is_manufactured_multi_section
,is_manufactured_permanently_affixed
,is_manufactured_single_section
,is_manufactured_titled
,is_manufactured_title_unknown
,is_manufactured_untitled
,is_lockoff_suite
,is_no_lockoff_suite
,is_two_stories_mbr_up
,is_three_stories_mbr_down
,is_three_stories_mbr_up
,is_two_stories_mbr_down
,has_no_primary_bath
,has_primary_bath
,is_manufactured_on_land
,is_zoning_non_conforming
,is_zoning_other
,is_zoning_site_condominium
,is_zoning_none
,pets_allowed_unkown
,pets_allowed_unspecified
,is_active_reactivated
,has_basement_eight_ft_ceiling
,has_basement_radon_mitigation
,has_basement_other_foundation
,has_basement_outside_entry
,has_basement_toilet_only
,has_basement_shower_only
,has_ductless_hp_mini_split_cooling
,has_heat_gas_pack
,has_heat_forced_gas
,bedroom_length
,bedroom_width
,primary_bedroom_length
,primary_bedroom_width
,originating_mls_copywrite
,has_lake_name_kahshe
,has_no_split_bedroom_plan
,sold_price_per_sqft
,is_not_condo_timeshare
,in_use_shortterm_rental

from stage.direct_idx_attribute_3 s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
where s.source_id in {} and t.target_listing_id is not NULL
"""

LISTING_ATTRIBUTE_CUSTOM_QUERY = """
select 
 t.source_id as source_id     ,
 t.batch_id as batch_id      ,
  t.target_listing_id   as listing_id  ,
  has_community_elevator,
  has_community_fitness_room,
  is_community_horses_allowed,
  has_community_horse_stables,
  is_community_maintenance_free,
  has_community_tennis_courts,
  is_exterior_brick,
  has_garage_1_carport,
  has_garage_1_car,
  has_garage_2_carports,
  has_garage_2_cars,
  has_garage_3_carports,
  has_garage_3_cars,
  is_property_high_rise,
  is_property_mid_rise,
  is_property_one_story,
  is_property_two_stories,
  is_property_three_stories,
  is_townhouse_2_3_floors,
  has_roof_metal,
  has_roof_shingle,
  has_roof_tile,
  has_waterfront_bay_harbor,
  has_waterfront_bayou,
  has_waterfront_beach_private,
  has_waterfront_beach_public,
  has_waterfront_brackish_water,
  has_waterfront_canal_brackish,
  has_waterfront_canal_freshwater,
  has_waterfront_canal_saltwater,
  has_waterfront_creek,
  has_waterfront_canal_with_lift,
  has_waterfront_gulf_ocean,
  has_waterfront_intracoastal_waterway,
  has_waterfront_lagoon,
  has_waterfront_lake,
  has_waterfront_lake_chain,
  has_waterfront_marina,
  has_waterfront_gulf_ocean_to_bay,
  has_waterfront_pond,
  has_waterfront_river,
  has_garage_1,
  has_garage_2,
  has_garage_3,
  has_garage_4,
  has_garage_5_plus,
  has_pool_above_ground,
  has_pool_diving,
  has_pool_fenced,
  has_pool_heated,
  has_pool_lap,
  has_pool_no_pool,
  has_pool_play,
  has_pool_private,
  has_pool_solar,
  has_pool_variable_speed_pump,
  has_pool_solar_thermal,
  has_parking_rv_gate,
  has_parking_rv,
  has_parking_rv_garage,
  is_level_ground,
  is_level_neighbor_above,
  is_level_neighbor_below,
  is_level_no_common_walls,
  is_level_one_common_wall,
  is_level_two_common_walls,
  is_level_three_common_walls,
  is_level_all_one_level,
  is_level_poolside,
  is_level_street_facing,
  is_level_two_levels,
  is_level_three_or_more_levels,
  has_community_recreation_room,
 t.y_creation_date,
 y_last_update_date,
  has_wateraccess_bay_harbor,
  has_wateraccess_bayou,
  has_wateraccess_beach_deeded,
  has_wateracecss_beach_private,
  has_wateraccess_beach_public,
  has_wateraccess_brackish_water,
  has_wateraccess_canal_brackish,
  has_wateraccess_canal_freshwater,
  has_wateraccess_canal_saltwater,
  has_wateraccess_creek,
  has_wateraccess_canal_with_lift,
  has_wateraccess_gulf_ocean,
  has_wateraccess_intracoastal_waterway,
  has_wateraccess_lagoon,
  has_wateraccess_lake,
  has_wateraccess_lake_chain,
  has_wateraccess_limited_access,
  has_wateraccess_marina,
  has_wateraccess_gulf_ocean_to_bay,
  has_wateraccess_pond,
  has_wateraccess_river,
  has_waterview_bay_harbor,
  has_waterview_bayou,
  has_waterview_bay_harbor_partial,
  has_waterview_beach,
  has_waterview_canal,
  has_waterview_creek,
  has_waterview_gulf_ocean_to_bay,
  has_waterview_gulf_ocean_full,
  has_waterview_gulf_ocean_partial,
  has_waterview_intracoastal_waterway,
  has_waterview_lagoon,
  has_waterview_lake,
  has_waterview_lake_chain,
  has_waterview_marina,
  has_waterview_pond,
  has_waterview_river,
  is_property_desc_adjacent_to_wash,
  has_property_desc_common_area_border,
  is_property_desc_border_pres_pub,
  has_property_desc_mountain_view,
  is_property_desc_north_south_exposure,
  imls_is_level_single,
imls_is_level_single_with_below_grade,
imls_is_level_split_entry,
imls_is_level_tri_level,
imls_is_level_tri_below_garage,
imls_is_level_two_levels,
imls_is_level_two_story_below_grade,
imls_is_level_single_with_upstairs_bonus,
imls_is_level_other,
gamls_is_exterior_brick_3_side,
gamls_is_exterior_brick_4_side,
gamls_is_exterior_stucco,
gamls_is_exterior_stucco_unknown,
gamls_is_basement_desc_bath_finished,
gamls_is_basement_desc_bath_stubbed,
gamls_is_basement_desc_finished_room,
gamls_is_basement_desc_full,
gamls_is_story_one,
gamls_is_story_one_half,
gamls_is_story_two,
gamls_is_story_two_plus,
gamls_is_story_multi_level,
gamls_is_story_split_foyer,
gamls_is_story_split_level,
  mtrmls_community_is_55_up_community,
  mtrmls_community_has_clubhouse,
  mtrmls_community_has_fitness_center,
  mtrmls_community_is_gated_community,
  mtrmls_community_has_park,
  mtrmls_community_has_playground,
  mtrmls_community_has_pool,
  mtrmls_community_has_tennis_courts,
mtrmls_fence_back,
mtrmls_fence_chain,
mtrmls_fence_dog_run,
mtrmls_fence_front_yard,
mtrmls_fence_full_yard,
mtrmls_fence_partial,
mtrmls_fence_privacy,
mtrmls_fence_split_rail,
mtrmls_waterfront_creek,
mtrmls_waterfront_lake_view,
mtrmls_waterfront_lake_front,
mtrmls_waterfront_pond,
mtrmls_waterfront_river_view,
mtrmls_waterfront_river_front,
  nwamls_has_carport_1_car,
  nwamls_has_garage_1_car,
  nwamls_has_carport_2_car,
  nwamls_has_garage_2_car,
  nwamls_has_carport_3_car,
  nwamls_has_garage_3_car,
  nwamls_has_carport_4_car,
  nwamls_has_garage_4_car,
  nwamls_has_one_level,
  nwamls_has_two_levels,
  nwamls_has_three_levels,
  nwamls_is_split_level,
  nwamls_is_tri_level,
  nwamls_is_waterfront_creek_stream_spring,
  nwamls_is_waterfront_lake_area,
  nwamls_is_waterfront_lake_front,
  nwamls_is_waterfront_pond,
  nwamls_is_waterfront_river_front,
  nwamls_is_waterfront_seasonal_view,
  nwamls_is_waterfront_wet_weather_creek,
  rayac_is_senior_community_50_plus,
  rayac_is_senior_community_55_plus,
  rayac_is_senior_community_62_plus,
  creb_features_central_air_conditioning,
  creb_features_balcony,
  creb_features_bar,
  creb_features_barbecue_built_in,
  creb_features_ceiling_10_ft,
  creb_features_ceiling_9_ft,
  creb_features_dance_floor,
  creb_features_deck,
  creb_features_dog_run_fenced_in,
  creb_features_fire_pit,
  creb_features_greenhouse,
  creb_features_handyman_special,
  creb_features_handicap_access,
  creb_features_handicap_interior_accessories,
  creb_features_hot_water_tank_energy_star,
  creb_features_tankless_hot_water,
  creb_features_insulation_upgraded,
  creb_features_low_flow_faucets_showerheads,
  creb_features_low_flow_dual_flush_toilets,
  creb_features_no_animal_home,
  creb_features_no_smoking_home,
  creb_features_open_beam,
  creb_features_patio,
  creb_features_programmable_thermostat,
  creb_features_porch,
  creb_features_sauna,
  creb_features_security_window_bars,
  creb_features_skylight,
  creb_features_sprinkler_system_fire,
  creb_features_sprinkler_system_underground,
  creb_features_steam_room,
  creb_features_sunroom,
  creb_features_swirlpool_bath_jacuzzi,
  creb_features_vacuum_system_roughed_in,
  creb_features_vaulted_ceiling,
  creb_features_vinyl_windows,
  creb_features_wood_windows,
  creb_features_wall_unit_built_in,
  mfrmls_community_airport_runway,
  mfrmls_community_association_rec_lease,
  mfrmls_community_association_rec_owned,
  mfrmls_community_boat_slip,
  mfrmls_community_buyer_approval_req,
  mfrmls_community_cable,
  mfrmls_community_card_entry,
  mfrmls_community_clubhouse,
  mfrmls_community_community_hot_tub_spa,
  mfrmls_community_community_boat_ramp,
  mfrmls_community_deed_restrictions,
  mfrmls_community_deed_restricted,
  mfrmls_community_dock,
  mfrmls_community_dock_boat_slip,
  mfrmls_community_elevators,
  mfrmls_community_fees_required,
  mfrmls_community_fishing_pier,
  mfrmls_community_fitness,
  mfrmls_community_gated_community,
  mfrmls_community_golf_carts_ok,
  mfrmls_community_golf_community,
  mfrmls_community_golf_course,
  mfrmls_community_handicap_modified,
  mfrmls_community_horses_allowed,
  mfrmls_community_horse_stables,
  mfrmls_community_irrigation_reclaimed_water,
  mfrmls_community_laundry_facility,
  mfrmls_community_lawn_pest_fertilizer,
  mfrmls_community_lawn_service_included,
  mfrmls_community_lobby_key_required,
  mfrmls_community_marina,
  mfrmls_community_maintenance_free,
  mfrmls_community_no_community_pool,
  mfrmls_community_no_deed_restriction,
  mfrmls_community_no_pool,
  mfrmls_community_not_all_amenities_included_in_hoa_fees,
  mfrmls_community_no_truck_rv_motorcycle_parking,
  mfrmls_community_optional_additional_fees,
  mfrmls_community_park,
  mfrmls_community_pest_control_included,
  mfrmls_community_playground,
  mfrmls_community_pool,
  mfrmls_community_private_boat_ramp,
  mfrmls_community_public_boat_ramp,
  mfrmls_community_pud,
  mfrmls_community_racquet_ball,
  mfrmls_community_recreation_building,
  mfrmls_community_sauna,
  mfrmls_community_security,
  mfrmls_community_shopping_center,
  mfrmls_community_shuffleboard,
  mfrmls_community_sidewalk,
  mfrmls_community_special_restrictions,
  mfrmls_community_storage,
  mfrmls_community_tenants_no_pets,
  mfrmls_community_tennis_courts,
  mfrmls_community_water_access,
  mfrmls_community_waterfront,
  mfrmls_community_wheelchair_access,
  mfrmls_community_waterfront_complex,
  mfrmls_community_waterfront_community,
  mfrmls_exterior_asbestos,
  mfrmls_exterior_block,
  mfrmls_exterior_brick,
  mfrmls_exterior_combination,
  mfrmls_exterior_concrete_block,
  mfrmls_exterior_curtain_wall,
  mfrmls_exterior_frame,
  mfrmls_exterior_icf_insulated_concrete_forms,
  mfrmls_exterior_log,
  mfrmls_exterior_metal,
  mfrmls_exterior_metal_frame,
  mfrmls_exterior_modular,
  mfrmls_exterior_on_piling,
  mfrmls_exterior_other,
  mfrmls_exterior_precast_concrete,
  mfrmls_exterior_siding,
  mfrmls_exterior_sip_structurally_insulated_panel,
  mfrmls_exterior_stem_wall,
  mfrmls_exterior_stone,
  mfrmls_exterior_stucco,
  mfrmls_exterior_tilt_up,
  mfrmls_exterior_tilt_up_walls,
  mfrmls_exterior_wood_frame,
  mfrmls_exterior_wood_frame_fsc_certified,
  mfrmls_pets_extra_large_101_plus_lbs,
  mfrmls_pets_large_61_100_lbs,
  mfrmls_pets_medium_36_60_lbs,
  mfrmls_pets_small_16_35_lbs,
  mfrmls_pets_very_small_under_15_lbs,
  mfrmls_water_extras_boathouse,
  mfrmls_water_extras_boat_ramp_private,
  mfrmls_water_extras_bridges_fixed,
  mfrmls_water_extras_davits,
  mfrmls_water_extras_dock_slip_1st_come,
  mfrmls_water_extras_dock_composite,
  mfrmls_water_extras_dock_concrete,
  mfrmls_water_extras_dock_covered,
  mfrmls_water_extras_dock_slip_deeded_offsite,
  mfrmls_water_extras_dock_slip_deeded_onsite,
  mfrmls_water_extras_dock_open,
  mfrmls_water_extras_dock_w_electric,
  mfrmls_water_extras_dock_wo_electric,
  mfrmls_water_extras_dock_wood,
  mfrmls_water_extras_dock_wo_water_supply,
  mfrmls_water_extras_dock_w_water_supply,
  mfrmls_water_extras_fishing_pier,
  mfrmls_water_extras_lift_covered,
  mfrmls_water_extras_lift,
  mfrmls_water_extras_private_lake_dues_req,
  mfrmls_water_extras_lock,
  mfrmls_water_extras_min_wake_zone,
  mfrmls_water_extras_boats_none_allowed,
  mfrmls_water_extras_bridges_no_fixed_bridges,
  mfrmls_water_extras_no_wake_zone,
  mfrmls_water_extras_powerboats_none_allowed,
  mfrmls_water_extras_riprap,
  mfrmls_water_extras_sailboat_water,
  mfrmls_water_extras_seawall_concrete,
  mfrmls_water_extras_seawall_other,
  mfrmls_water_extras_skiing_allowed,
  mfrmls_prop_desc_1_level,
  mfrmls_prop_desc_1st_floor_multi_story,
  mfrmls_prop_desc_2_levels,
  mfrmls_prop_desc_2nd_floor_multi_story,
  mfrmls_prop_desc_3_levels,
  mfrmls_prop_desc_3rd_floor_above_multi_story,
  mfrmls_prop_desc_4_levels,
  mfrmls_prop_desc_attached,
  mfrmls_prop_desc_detached,
  mfrmls_prop_desc_efficiency,
  mfrmls_prop_desc_elevated,
  mfrmls_prop_desc_end_unit,
  mfrmls_prop_desc_four_story,
  mfrmls_prop_desc_ground_floor_unit,
  mfrmls_prop_desc_ground_level,
  mfrmls_prop_desc_high_rise,
  mfrmls_prop_desc_in_m_h_community,
  mfrmls_prop_desc_mid_rise,
  mfrmls_prop_desc_one_story,
  mfrmls_prop_desc_out_of_m_h_community,
  mfrmls_prop_desc_penthouse,
  mfrmls_prop_desc_split_level,
  mfrmls_prop_desc_three_story,
  mfrmls_prop_desc_townhouse_2_3_floors,
  mfrmls_prop_desc_tri_level,
  mfrmls_prop_desc_two_story,
  rapb_membership_none,
  rapb_membership_required,
  rapb_membership_equity_purch_req,
  rapb_membership_golf_equity_avail,
  rapb_membership_golf_equity_included,
  rapb_membership_golf_purchase,
  rapb_membership_other_membership_avail,
  rapb_membership_other_membership_incl,
  rapb_membership_tennis_membership_avail,
  rapb_membership_tennis_membership_incl,
  tmls_design_is_story_one,
  tmls_design_is_story_one_half,
  tmls_design_is_story_two,
  tmls_design_is_story_two_plus,
  tmls_design_is_story_three,
  pacmls_levels_is_story_one,
  pacmls_levels_is_story_one_w_basement,
  pacmls_levels_is_story_one_half,
  pacmls_levels_is_story_two,
  pacmls_levels_is_story_two_w_basement,
  pacmls_levels_is_story_three,
  pacmls_levels_is_story_four,
  pacmls_levels_is_story_two_plus,
  pacmls_levels_is_story_one_w_bonus,
  pacmls_levels_is_story_tri_level,
  has_garage_none,
  has_garage_5,
  creb_basement_crawl_space,
  creb_basement_dugout,
  creb_basement_full,
  creb_basement_none,
  creb_basement_partial,
  creb_basement_walkout,
  creb_unit_exposure_east,
  creb_unit_exposure_north,
  creb_unit_exposure_northeast,
  creb_unit_exposure_northwest,
  creb_unit_exposure_south,
  creb_unit_exposure_southeast,
  creb_unit_exposure_southwest,
  creb_unit_exposure_west,
  creb_is_bathroom_ensuite,
  armls_prop_desc_is_adjacent_to_wash,
  armls_prop_desc_has_common_area_border,
  armls_prop_desc_is_border_pres_pub,
  armls_prop_desc_has_mountain_view,
  armls_prop_desc_is_north_south_exposure,
glvmls_accessibility_feature_Bath_Grab_Bars,
glvmls_accessibility_feature_Bath_Lever_Faucets,
glvmls_accessibility_feature_Bath_Roll_In_Shower,
glvmls_accessibility_feature_Bath_Roll_Under_Sink,
glvmls_accessibility_feature_Bath_Raised_Toilet,
glvmls_accessibility_feature_Closet_Bars_15_48_in,
glvmls_accessibility_feature_Dr_Access_32_in_Wide,
glvmls_accessibility_feature_Exterior_Curb_Cuts,
glvmls_accessibility_feature_Hallways_36_in_Wide,
glvmls_accessibility_feature_Hard_Low_Nap_Floors,
glvmls_accessibility_feature_Ktch_Apps_Low_Secure,
glvmls_accessibility_feature_Ktch_Low_Cabinetry,
glvmls_accessibility_feature_Ktch_Low_Counters,
glvmls_accessibility_feature_Ktch_Modified_Range,
glvmls_accessibility_feature_Ktch_Roll_Under_Sink,
glvmls_accessibility_feature_Lever_Handles,
glvmls_accessibility_feature_Ramps,
glvmls_accessibility_feature_Remote_Devices,
glvmls_accessibility_feature_Stair_Lifts,
glvmls_accessibility_feature_Tactile_Visual_Mrkers,
glvmls_accessibility_feature_Zero_Grade_Entry,
glvmls_energy_desc_Awnings,
glvmls_energy_desc_Dual_Pane_Windows,
glvmls_energy_desc_Insulated_Door,
glvmls_energy_desc_Insulated_Windows,
glvmls_energy_desc_Low_E_Windows,
glvmls_energy_desc_None,
glvmls_energy_desc_Other,
glvmls_energy_desc_Solar_Panels,
glvmls_energy_desc_Solar_Water_Heater,
glvmls_energy_desc_Solar_Screens,
glvmls_energy_desc_Storm_Doors,
glvmls_energy_desc_Tinted_Windows,
glvmls_energy_desc_Triple_Panel_Windows,
glvmls_energy_desc_Roof_Turbines,
has_waterview_gulf,
has_waterview_gulf_pass,
has_waterview_sound,
has_waterview_stream,
has_waterview_coastal_dune_lakes,
has_waterfront_canal,
has_waterfront_gulf_pass,
ecar_waterfront_riparianrights,
ecar_waterfront_shore_beach,
ecar_waterfront_shore_natural,
ecar_waterfront_shore_rip_rap,
ecar_waterfront_shore_seawall,
has_waterfront_sound,
has_waterfront_stream,
ecar_waterfront_coastal_dune_lakes,
creb_parking_single_carport,
creb_parking_single_garage_attached,
creb_parking_single_garage_detached,
creb_parking_single_indoor,
creb_parking_220_volt_wiring,
creb_parking_double_carport,
creb_parking_double_garage_attached,
creb_parking_double_garage_detached,
creb_parking_double_indoor,
creb_parking_2_outdoor_stalls,
creb_parking_triple_garage_attached,
creb_parking_triple_garage_detached,
creb_parking_quad_or_more_attached,
creb_parking_quad_or_more_detached,
creb_parking_breezeway,
creb_parking_front_drive_access,
creb_parking_front_and_rear_drive_access,
creb_parking_heated,
creb_parking_insulated,
creb_parking_no_assigned_parking,
creb_parking_no_garage,
creb_parking_over_sized,
creb_parking_parking_pad_cement_or_paved,
creb_parking_parkade,
creb_parking_parking_pad_gravel,
creb_parking_rear_drive_access,
creb_parking_rv_parking,
creb_parking_shop,
creb_parking_stall,
creb_parking_tandem,
creb_parking_underground,
has_exterior_metal_frame,
scar_garage_1_car_detached,
scar_garage_2_cars_detached,
scar_garage_3_cars_detached,
scar_property_status_auction,
scar_property_status_short_sale,
scar_property_status_hud,
scar_property_status_relo_company,
scar_property_status_bank_owned,
scar_property_status_probate_listing,
scar_property_status_standard,
scar_waterfront_ocean,
scar_waterfront_banana_river,
scar_waterfront_indian_river,
scar_waterfront_newfound_harbor,
scar_waterfront_sykes_creek,
scar_waterfront_crane_creek,
scar_waterfront_lake_pond,
scar_waterfront_deeded_access_only,
scar_waterfront_grand_central,
crmls_lot_sqft_6500_9999,
crmls_lot_sqft_10000_19999,
crmls_lot_sqft_20000_39999,
crmls_lot_sqft_40000_plus,
has_waterview_ocean,
rapb_horse_property_tack_room,
rapb_horse_property_feed_room,
rapb_horse_property_community_stall,
rapb_horse_property_water_electric,
rapb_horse_property_boarding_allowed,
rapb_horse_property_stable,
rapb_horse_property_ring,
rapb_horse_property_paddocks,
rapb_horse_property_grooms_quarters,
rapb_horse_property_owners_apartment,
rapb_horse_property_office,
rapb_horse_property_lounge,
rapb_horse_property_mirror,
rapb_horse_property_bridle_path_trails,
rapb_horse_property_grass_field,
rapb_horse_property_center_aisle,
rapb_horse_property_shed_row,
rapb_horse_property_covered_ring,
rapb_horse_property_regulation_dressage,
rapb_horse_property_fly_system,
rapb_horse_property_wash_rack,
rapb_horse_property_washer_dryer_hookup,
scar_waterfront_canal_non_navigation,
scar_waterfront_canal_navigation,
scar_waterfront_grand_canal,
has_waterfront_ocean,
hmls_is_floorplan_one_plus_half,
hmls_is_floorplan_raised_one_plus_half,
hmls_is_floorplan_raised_ranch,
hmls_is_floorplan_ranch,
hmls_is_floorplan_reverse_story_one_plus_half,
hmls_is_floorplan_side_split,
hmls_is_floorplan_split_entry,
hmls_is_floorplan_tri_level,
hmls_is_floorplan_two_stories,
hmls_is_floorplan_three_plus_stories,
hmls_is_floorplan_atrium_split,
hmls_is_floorplan_bungalow,
hmls_is_floorplan_california_split,
hmls_is_floorplan_earth_contact,
hmls_is_floorplan_front_back_split,
hmls_is_floorplan_loft,
  scaor_community_has_activities_director ,
  scaor_community_has_assigned_parking ,
  scaor_basement_has_basement_cellar ,
  scaor_basement_has_basement_crawl_space ,
  scaor_basement_has_basement_wine_cellar ,
  scaor_community_has_bike_trail ,
  scaor_community_has_bocce_court ,
  scaor_sewer_has_cesspool_sewer ,
  scaor_community_has_basketball_courts ,
  scaor_community_has_cable_tv ,
  scaor_community_has_community_center ,
  scaor_community_has_custodial_services ,
  scaor_community_has_day_care ,
  scaor_community_has_dock ,
  scaor_community_has_laundry_facility ,
  scaor_community_has_lawn_service ,
  scaor_community_has_marina ,
  scaor_community_has_playground ,
  scaor_community_has_putting_green ,
  scaor_community_has_sauna ,
  scaor_community_has_security ,
  scaor_community_has_sidewalk ,
  scaor_community_has_water_access ,
  scaor_sewer_has_dentrification_system ,
  scaor_community_has_game_room ,
  scaor_sewer_has_grinder_pump ,
  scaor_sewer_has_holding_tank ,
  scaor_sewer_has_lift_pump ,
  scaor_sewer_has_mound_septic ,
  scaor_sewer_has_peat_system ,
  scaor_community_has_pet_park ,
  scaor_sewer_has_pressurized_system ,
  scaor_sewer_has_private_central ,
  scaor_sewer_has_public_central ,
  scaor_community_has_satellite_tv ,
  scaor_sewer_is_septic ,
  scaor_sewer_is_private_central ,
  scaor_sewer_is_public_central ,
  scaor_sewer_is_public_ctrl_avail ,
  scaor_community_has_walk_path ,
  scaor_water_has_booster_pump ,
  scaor_water_has_impact_fee ,
  scaor_water_is_non_domestic ,
  scaor_water_is_private_central ,
  scaor_water_is_public_central ,
  scaor_water_has_tap_fee ,
  scaor_water_has_tower ,
  scaor_water_has_well ,
  scaor_water_has_well_shared ,
  scaor_waterfront_is_tidal_wetland ,
  scaor_waterfront_tidal_wetland ,
  scaor_waterview_tidal_wetland ,
  scaor_waterview_non_tidal_wetland ,
  scaor_basement_is_finished ,
  scaor_basement_is_full ,
  scaor_basement_has_inside_stairs ,
  scaor_basement_has_outside_stairs ,
  scaor_basement_is_partial ,
  scaor_basement_is_partially_finished ,
  scaor_basement_is_partially_furnished ,
  scaor_basement_is_unfinished ,
  scaor_basement_walkout_level ,
  scaor_ownership_is_coop ,
  scaor_is_within_city_limits ,
  scaor_is_within_historic_district ,
  scaor_is_raquetball_community ,
  scaor_waterfront_is_navigable ,
  scaor_waterview_is_navigable ,
gaar_cooling_attic_fan	,
gaar_cooling_evaporative	,
gaar_cooling_heat_pump	,
gaar_cooling_refrigerated	,
gaar_cooling_roof_turbine	,
gaar_cooling_two_plus_units	,
gaar_cooling_window_units	,
gaar_has_private_pool	,
gaar_roof_bitumen	,
gaar_roof_composition	,
gaar_roof_flat	,
gaar_roof_foam	,
gaar_roof_mansard	,
gaar_roof_metal	,
gaar_roof_mixed	,
gaar_roof_pitched	,
gaar_roof_pitched_flat	,
gaar_roof_positive_pitched	,
gaar_roof_rolled_roofing	,
gaar_roof_rubber_membrane	,
gaar_roof_shake	,
gaar_roof_shingle	,
gaar_roof_slate	,
gaar_roof_tar_crushed_back	,
gaar_roof_tar_gravel	,
gaar_roof_tile	,
harmls_has_study_library	,
harmls_has_media_room	,
harmls_has_gameroom_up	,
harmls_has_gameroom_down	,
harmls_has_private_pool	,
harmls_has_greenbelt	,
hhimls_view_deep_water,
hhimls_view_golf,
hhimls_view_harbor,
hhimls_view_lagoon,
hhimls_view_lake,
hhimls_view_landscape,
hhimls_view_marsh,
hhimls_view_ocean,
hhimls_view_pool,
hhimls_view_river,
hhimls_view_sound,
hhimls_view_wooded,
scaor_dock_against_bullhead,
scaor_dock_multiple_slips,
scaor_dock_none,
scaor_dock_other,
scaor_dock_pier,
scaor_dock_single_slip,
scaor_extra_unit_1bdrm,
scaor_extra_unit_2bdrm,
scaor_extra_unit_3bdrm,
scaor_extra_unit_basement,
scaor_extra_unit_care_taker,
scaor_extra_unit_detached,
scaor_extra_unit_efficiency,
scaor_extra_unit_garage_apt,
scaor_extra_unit_in_law_suite,
scaor_extra_unit_2nd_flr_up,
scaor_extra_unit_2nd_flr_efficiency,
scaor_extra_unit_prof_office,
scaor_extra_unit_3rd_flr_efficiency,
scaor_extra_unit_separate_entrance,
scaor_extra_unit_1st_flr,
scaor_extra_unit_1st_flr_efficiency,
scaor_extra_unit_tenant_unit,
scaor_map_section_1113b,
scaor_map_section_11313,
scaor_map_section_eaofr,
scaor_map_section_weofr,
has_waterview_basin,
has_waterview_bay,
has_waterview_intersecting_canal,
has_waterview_mangroves,
rase_garage_type_tandem_garage	,
rase_garage_type_tuck_under_garage	,
rase_garage_amenties_extra_pad	,
rase_garage_amenities_oversized	,
rase_garage_amenities_room_for_garage	,
rase_garage_amenities_drain	,
rase_garage_amenities_heater	,
rase_fireplace_gas	,
rase_fireplace_wood_burning	,
rase_fireplace_free_standing_stove	,
rase_hoa_amenities_community_pool	,
rase_hoa_amenities_tennis	,
rase_hoa_amenities_golf	,
rase_hoa_amenities_comm_center	,
rase_hoa_amenities_snow_removal	,
rase_hoa_amenities_lawn_care	,
rase_hoa_amenities_sauna	,
rase_hoa_amenities_exercise_room	,
rase_hoa_amenities_hobby_room	,
rase_hoa_amenities_exterior_bldg_maint	,
rase_hoa_amenities_road_maint	,
rase_int_feat_tray_ceiling	,
rase_int_feat_skylights	,
rase_int_feat_3_plus_bdrm_same_level	,
rase_site_feat_rv_storage	,
rase_site_feat_shade_trees	,
rase_site_feat_landscaping	,
rase_site_feat_invisible_pet_fence	,
rase_site_feat_partial_fence	,
rase_site_feat_privacy_fence	,
rase_site_feat_chain_link	,
rase_site_feat_lawn_sprinkler_ungrd	,
rase_site_feat_cable_tv	,
rase_site_feat_satellite_tv	,
rase_site_feat_deck	,
rase_site_feat_covered_deck	,
rase_site_feat_yard_play_set	,
rase_site_feat_patio	,
rase_site_feat_covered_patio	,
rase_site_feat_porch	,
rase_site_feat_3_season_porch	,
rase_site_feat_covered_front_porch	,
rase_site_feat_concrete_drive	,
rase_site_feat_asphalt_drive	,
rase_site_feat_gravel_drive	,
rase_site_feat_dirt_drive	,
gsrein_outbuilding_shed	,
gsrein_outbuilding_guest_house	,
gsrein_outbuilding_barn	,
gsrein_outbuilding_workshop	,
gsrein_outbuilding_green_house	,
gsrein_outbuilding_cabana	,
gsrein_condition_new	,
gsrein_condition_excellent	,
gsrein_condition_vrgd	,
gsrein_waterfront_water_access_community	,
gsrein_foundation_raised	,
gsrein_foundation_slab	,
gsrein_style_acadian	,
gsrein_style_camelback	,
gsrein_style_camp	,
gsrein_style_center_hall	,
gsrein_style_colonial	,
gsrein_style_contemporary	,
gsrein_style_cottage	,
gsrein_style_creoletownhouse	,
gsrein_style_farm	,
gsrein_style_french_country	,
gsrein_style_french_provincial	,
gsrein_style_georgian	,
gsrein_style_mobile_home	,
gsrein_style_modular	,
gsrein_style_shot_gun	,
gsrein_style_traditional,
ccimls_miles_to_beach_0_p1,
ccimls_miles_to_beach_p1_p3,
ccimls_miles_to_beach_p3_p5,
ccimls_miles_to_beach_p5_1,
ccimls_miles_to_beach_1_2,
ccimls_miles_to_beach_2_plus,
gsrein_condition_average,
gsrein_condition_fair,
gsrein_condition_poor,
scaor_oldfather_public_sewer,
scaor_oldfather_style_not_trailer,
scaor_oldfather_no_land_lease,
scaor_oldfather_comm_55_plus,
scaor_oldfather_bayfront,
scaor_oldfather_comm_lawn_care,
scaor_oldfather_oceanfront,
scaor_oldfather_private_pool,
scaor_oldfather_comm_marina,
scaor_oldfather_new_construction,
scaor_oldfather_waterview,
scaor_oldfather_first_flr_master,
scaor_oldfather_boat_slip,
scaor_oldfather_has_basement,
car_waterfront_bayside_waterfront,
car_waterfront_bayside_interior,
car_waterfront_ocean_block,
car_waterfront_ocean_view,
car_waterfront_direct_oceanfront,
nwmls_style_1_story,
nwmls_style_2_story,
nwmls_style_3_story,
nwmls_style_split_entry,
nwmls_style_multi_level,
nwmls_style_1_story_w_basement,
nwmls_style_1_plus_half_story_w_basement as nwmls_style_1_p_half_story_w_basement,
nwmls_style_2_story_w_basement,
scaor_is_half_acre_plus,
gamls_has_basement_slab_none,
tbor_timbertype_partial,
tbor_timbertype_heavily_wooded,
tbor_timbertype_hardwood,
tbor_timbertype_softwood,
tbor_timbertype_mixed,
tbor_timbertype_none,
tbor_waterfront_pond_lake,
fmls_has_basement_slab_none,
crmls_assessments_buyer_to_assume,
crmls_assessments_buyer_to_verify,
crmls_assessments_seller_to_pay,
crmls_assessments_mello_roos,
crmls_assessments_none,
crmls_assessments_sewer_assessments,
crmls_assessments_sewer_bonds,
crmls_assessments_special_assessments,
sandicor_frontage_bay,
sandicor_frontage_blm,
sandicor_frontage_canyon,
sandicor_frontage_freeway,
sandicor_frontage_golf_course,
sandicor_frontage_lagoon_estuary,
sandicor_frontage_lake_river,
sandicor_frontage_military_land,
sandicor_frontage_ocean_bluff,
sandicor_frontage_ocean_sand,
sandicor_frontage_open_space,
svvar_style_mobile,
tbor_timbertype_cluster,
glvmls_has_lv_strip_view,
recolorado_basementtype_apartment,
recolorado_basementtype_depends_on_lot,
recolorado_basementtype_cellar,
recolorado_basementtype_garden_level,
recolorado_basementtype_none,
recolorado_basementtype_standard,
recolorado_basementtype_walkout,
normls_basement_common,
normls_basement_crawl,
normls_basement_full,
normls_basement_none,
normls_basement_other,
normls_basement_partial,
normls_basement_slab,
normls_basement_unfinished,
normls_basement_walkout,
normls_style_cluster,
normls_style_conventional,
normls_style_half_duplex,
recolorado_arch_style_5_plus_plex,
recolorado_arch_style_cluster,
recolorado_arch_style_denver_square,
recolorado_arch_style_paired_duplex,
recolorado_arch_style_double_wide,
recolorado_arch_style_mid_century_modern,
recolorado_arch_style_other,
recolorado_arch_style_quadplex,
recolorado_arch_style_studio,
recolorado_arch_style_single_wide,
recolorado_arch_style_triplex,
recolorado_arch_style_triple_wide,
recolorado_basement_size_depend_on_lot,
recolorado_basement_size_full,
recolorado_basement_size_none,
recolorado_basement_size_partial,
giar_frontage_deep_water,
giar_frontage_golf_frontage,
giar_frontage_golf_view,
giar_frontage_lake_lagoon_frontage,
giar_frontage_lake_lagoon_view,
giar_frontage_marsh_frontage,
giar_frontage_marsh_view,
giar_frontage_natural,
giar_frontage_ocean,
giar_frontage_ocean_view,
giar_frontage_park,
giar_frontage_park_view,
giar_frontage_residential,
giar_frontage_river,
giar_frontage_river_view,
giar_frontage_stream,
giar_frontage_tidal_creek,
giar_frontage_waterfront,
giar_frontage_other,
giar_facilities_bbq_grill,
giar_facilities_beach,
giar_facilities_boat_ramp,
giar_facilities_cable_tv,
giar_facilities_community_room,
giar_facilities_deed_access,
giar_facilities_deep_water,
giar_facilities_dock,
giar_facilities_elevators,
giar_facilities_exercise_room,
giar_facilities_fenced,
giar_facilities_furnished,
giar_facilities_game_room,
giar_facilities_gated_community,
giar_facilities_golf,
giar_facilities_handicap_provisions,
giar_facilities_hot_tub,
giar_facilities_landscaped,
giar_facilities_laundry,
giar_facilities_marina,
giar_facilities_member_app_rights,
giar_facilities_min_rental_period,
giar_facilities_no_pets_allowed,
giar_facilities_no_rental,
giar_facilities_no_short_term,
giar_facilities_ocean_front,
giar_facilities_ocean_view,
giar_facilties_pavilion_gazebo,
giar_facilities_pet_friendly,
giar_facilities_picnic_area,
giar_facilities_playground,
giar_facilities_pond,
giar_facilities_pool,
giar_facilities_security_system,
giar_facilities_separate_storage,
giar_facilities_spa,
giar_facilities_tennis_courts,
giar_facilities_other,
giar_exterior_balcony,
giar_exterior_columns,
giar_exterior_deck_covered,
giar_exterior_deck_open,
giar_exterior_hot_tub,
giar_exterior_hurricane_shutters,
giar_exterior_needs_work,
giar_exterior_outside_shower,
giar_exterior_patio_covered,
giar_exterior_patio_enclosed,
giar_exterior_patio_open,
giar_exterior_porch,
giar_exterior_porch_screened,
giar_exterior_renovated,
giar_exterior_satellite_dish,
giar_exterior_sauna_steam,
giar_exterior_tv_antenna,
giar_exterior_other,
giar_lot_acreage_10_plus,
giar_lot_acreage_1_5,
giar_lot_acreage_5_10,
giar_lot_addl_land_avail,
giar_lot_aerials_topo_avail,
giar_lot_bbq_pit_grill,
giar_lot_beach_dunes,
giar_lot_boat_dock,
giar_lot_boat_ramp,
giar_lot_boat_slip,
giar_lot_boathouse,
giar_lot_boatlift,
giar_lot_bulkhead_seawall,
giar_lot_cabana,
giar_lot_cleared,
giar_lot_corner,
giar_lot_covenants,
giar_lot_cul_de_sac,
giar_lot_curb_and_gutter,
giar_lot_dead_end,
giar_lot_detached_bldg,
giar_lot_dock_slip_only,
giar_lot_easements,
giar_lot_fenced_back_yard,
giar_lot_fenced_chain_link,
giar_lot_fenced_lot_all,
giar_lot_fenced_lot_part,
giar_lot_fenced_privacy,
giar_lot_greenhouse,
giar_lot_guest_house,
giar_lot_interior,
giar_lot_irregular,
giar_lot_irrigation_system,
giar_lot_land_lease,
giar_lot_landscaped,
giar_lot_level,
giar_lot_low,
giar_lot_marsh_land,
giar_lot_pavilion_gazebo,
giar_lot_pond,
giar_lot_pool_above_ground,
giar_lot_pool_enclosed,
giar_lot_pool_heated,
giar_lot_pool_house,
giar_lot_pool_in_ground,
giar_lot_privacy,
giar_lot_prop_corners_marked,
giar_lot_sidewalk,
giar_lot_soil_map,
giar_lot_storm_sewer,
giar_lot_subdiv_recorded,
giar_lot_subdiv_unrecorded,
giar_lot_survey_avail,
giar_lot_tennis_court,
giar_lot_workshop,
giar_lot_wetlands,
giar_lot_will_divide,
giar_lot_wooded,
giar_lot_woods_trail,
giar_lot_yard_well,
giar_lot_zero_dot_line,
giar_lot_other_see_remarks,
giar_interior_additional_bedroom_suite,
giar_interior_allowance_carpet,
giar_interior_bonus_room,
giar_interior_breakfast_bar,
giar_interior_ceilings_above_9_ft,
giar_interior_converted_garage,
giar_interior_formal_dining,
giar_interior_foyer,
giar_interior_laminate_flooring,
giar_interior_recessed_lighting,
giar_interior_pantry,
giar_interior_split_bedroom,
gbrar_exterior_balcony,
gbrar_exterior_barn,
gbrar_exterior_cabana,
gbrar_exterior_deck,
gbrar_exterior_gas_propane_grill,
gbrar_exterior_gazebo,
gbrar_exterior_generator_partial,
gbrar_exterior_generator_whole_house,
gbrar_exterior_greenhouse,
gbrar_exterior_guest_house,
gbrar_exterior_hot_tub,
gbrar_exterior_kennel,
gbrar_exterior_outdoor_fireplace_pit,
gbrar_exterior_outside_kitchen,
gbrar_exterior_patio_covered,
gbrar_exterior_patio_enclosed,
gbrar_exterior_patio_open,
gbrar_exterior_patio_screened,
gbrar_exterior_porch,
gbrar_exterior_storage_shed_bldg,
gbrar_exterior_tennis_court,
gbrar_exterior_workshop,
gbrar_fireplace_1,
gbrar_fireplace_2,
gbrar_fireplace_3,
gbrar_fireplace_double_sided,
gbrar_fireplace_gas_logs,
gbrar_fireplace_masonry,
gbrar_fireplace_other,
gbrar_fireplace_pre_fab,
gbrar_fireplace_ventless,
gbrar_fireplace_wood_burning,
gbrar_foundation_other,
gbrar_foundation_piers,
gbrar_foundation_piling_stilt,
gbrar_foundation_slab_post_tension,
gbrar_foundation_slab_traditional,
gbrar_garage_unenclosed,
gbrar_interior_built_in_bookcases,
gbrar_interior_ceilings_above_9_feet,
gbrar_interior_ceilings_varied_heights,
gbrar_interior_computer_nook,
gbrar_interior_crown_moulding,
gbrar_interior_gas_stove_con,
gbrar_interior_in_law_suite,
gbrar_interior_wet_bar,
gbrar_lot_corner,
gbrar_lot_cul_de_sac,
gbrar_lot_dead_end,
gbrar_lot_golf_course_front,
gbrar_lot_horse_property,
gbrar_lot_waterfront,
gbrar_lot_wooded,
gbrar_lot_zero_line,
gbrar_pool_above_ground,
gbrar_pool_fiberglass,
gbrar_pool_gunite,
gbrar_pool_inground,
gbrar_pool_lined,
gbrar_pool_saltwater,
gbrar_subdivision_club_house,
gbrar_subdivision_community_pool,
gbrar_subdivision_gated_community,
gbrar_subdivision_golf_course,
gbrar_subdivision_health_club,
gbrar_subdivision_park,
gbrar_subdivision_playground,
gbrar_subdivision_tennis_courts,
gbrar_waterfront_bayou_river,
gbrar_waterfront_lake,
gbrar_waterfront_view_water,
gbrar_waterfront_walk_to_river,
gbrar_waterfront_water_access,
recolorado_structural_style_other_multi_unit,
recolorado_structural_style_townhouse,
recolorado_structural_style_tri_level,
recolorado_structural_style_3_story,
recolorado_structural_style_2_story,
recolorado_structural_style_3_plus_story,
recolorado_structural_style_bi_level,
recolorado_structural_style_condo,
recolorado_structural_style_multi_level,
recolorado_structural_style_multi_story,
recolorado_structural_style_other,
fncmls_misc_crawl_space,
fncmls_misc_slab_foundation,
fncmls_misc_finished_bonus_room,
fncmls_misc_unfinished_bonus_room,
fncmls_foreclosure_bank_owned,
fncmls_foreclosure_corp_owned,
fncmls_foreclosure_filed,
fncmls_foreclosure_hud_owned,
fncmls_foreclosure_va_owned,
has_waterview_freshwater,
has_waterview_saltwater,
sibor_style_garage_to_garage,
sibor_style_is_hi_ranch_style,
sibor_style_other,
sibor_yard_back,
sibor_yard_front,
sibor_yard_side,
sibor_yard_none,
sibor_hoa_incl_clubhouse,
sibor_hoa_incl_health_club,
sibor_hoa_incl_marina,
sibor_hoa_incl_electric,
sibor_hoa_incl_gas,
sibor_hoa_incl_hot_water,
sibor_hoa_incl_tennis,
sibor_hoa_incl_outside_maint,
sibor_hoa_incl_playground,
sibor_hoa_incl_pool,
sibor_hoa_incl_sewer,
sibor_hoa_incl_snow_removal,
sibor_hoa_incl_taxes,
sibor_siding_all_brick,
sibor_siding_aluminum,
sibor_siding_asbestos,
sibor_siding_part_brick,
sibor_siding_stone,
sibor_siding_stucco,
sibor_siding_vinyl,
sibor_siding_wood,
sibor_basement_desc_finished,
sibor_basement_desc_legal_apt,
sibor_basement_desc_none,
sibor_basement_desc_other,
sibor_basement_desc_partially_finished,
sibor_basement_desc_unfinished,
sibor_basement_type_crawl,
sibor_basement_type_full,
sibor_basement_type_none,
sibor_basement_type_other,
sibor_basement_type_partial,
sibor_garage_location_builtin,
sibor_garage_location_none,
sibor_parking_assigned,
sibor_parking_carport,
sibor_parking_off_street,
sibor_parking_on_street,
rmls_has_rv_parking,
sibor_unit_2_not_leased,
sibor_unit_2_leased,
sibor_unit_2_location_basement,
sibor_unit_2_location_level_1,
sibor_unit_2_location_level_2,
sibor_unit_2_location_other,
sibor_unit_2_location_sep_unit,
sibor_unit_2_rent,
sibor_unit_2_bedrooms,
sibor_unit_2_rooms,
sibir_unit_2_full_baths,
sibor_unit_2_3q_baths,
sibor_unit_2_half_baths,
sibor_subtype_apartment,
sibor_subtype_sf_att,
sibor_subtype_sf_det,
sibor_subtype_sf_semi_att,
sibor_subtype_2fam_att,
sibor_subtype_2fam_det,
sibor_subtype_2fam_semi_att,
rapb_hopa_no,
rapb_hopa_yes_verified,
rapb_hopa_yes_unverified,
sibor_region_mid_island,
sibor_region_north,
sibor_region_south,
rmls_type_is_townhouse,
rmls_type_is_planned,
rmls_type_is_part_owned,
rmls_type_is_manufactured_on_land,
rmls_type_is_manufactured,
rmls_type_is_floating_home,
rmls_type_is_detached,
rmls_type_is_condo,
rmls_type_is_co_op,
rmls_type_is_attached,
recolorado_bedrooms_main,
recolorado_bedrooms_upper,
recolorado_bathrooms_main,
recolorado_bathrooms_upper,
realcomp_water_source_private_well,
realcomp_water_source_city,
realcomp_sewer_septic,
realcomp_sewer_sanitary,
realcomp_basementtype_walkout,
realcomp_basementtype_daylight,
has_garage_3_plus,
has_lake_river_privs,
hudmls_heat_cool_central_ac,
hudmls_heat_cool_window_ac,
hudmls_heat_cool_wall_ac,
hudmls_heat_cool_baseboard,
hudmls_heat_cool_electric,
hudmls_heat_cool_gas,
hudmls_heat_cool_gas_on_gas,
hudmls_heat_cool_hot_water,
hudmls_heat_cool_heat_pump,
hudmls_heat_cool_hot_air,
hudmls_heat_cool_oil,
hudmls_heat_cool_radiators,
hudmls_heat_cool_steam,
hudmls_heat_cool_other,
hudmls_common_doorman,
hudmls_common_security,
hudmls_common_health_club,
hudmls_common_community_room,
hudmls_common_sauna,
hudmls_common_exercise_room,
hudmls_common_jacuzzi,
hudmls_common_play_area,
hudmls_common_marina,
hudmls_common_shops_on_premises,
hudmls_common_elevator,
hudmls_common_wash_dry_room,
hudmls_common_storage,
hudmls_misc_new_york_view,
hudmls_misc_river_view,
hudmls_misc_near_train,
hudmls_misc_near_shopping,
hudmls_misc_near_bus,
hudmls_misc_near_parks,
hudmls_misc_near_schools,
hudmls_misc_near_path,
naar_3_br_1_level,
naar_4_br_1_level,
galmls_pool_type_personal,
galmls_pool_type_community,
realcomp_is_bi_level,
realcomp_is_tri_level,
realcomp_is_quad_level,
ccimls_flex_waterfront_bay,
ccimls_flex_waterfront_beach,
ccimls_flex_waterfront_bog,
ccimls_flex_waterfront_canal,
ccimls_flex_waterfront_creek,
ccimls_flex_waterfront_deep,
ccimls_flex_waterfront_fresh,
ccimls_flex_waterfront_harbor,
ccimls_flex_waterfront_lake,
ccimls_flex_waterfront_marina,
ccimls_flex_waterfront_marsh,
ccimls_flex_waterfront_nantucket,
ccimls_flex_waterfront_ocean,
ccimls_flex_waterfront_private,
ccimls_flex_waterfront_public,
ccimls_flex_waterfront_river,
ccimls_flex_waterfront_salt,
ccimls_flex_waterfront_sound,
ccimls_flex_waterfront_other,
ccimls_flex_style_aframe,
ccimls_flex_style_antique,
ccimls_flex_style_bungalow,
ccimls_flex_style_cape,
ccimls_flex_style_colonial,
ccimls_flex_style_contemporary,
ccimls_flex_style_cottage,
ccimls_flex_style_english,
ccimls_flex_style_farm,
ccimls_flex_style_federal,
ccimls_flex_style_gambrel,
ccimls_flex_style_garden,
ccimls_flex_style_garrison,
ccimls_flex_style_greek,
ccimls_flex_style_log,
ccimls_flex_style_other,
ccimls_flex_style_post,
ccimls_flex_style_raised,
ccimls_flex_style_ranch,
ccimls_flex_style_saltbox,
ccimls_flex_style_shingle,
ccimls_flex_style_split,
ccimls_flex_style_tri,
ccimls_flex_style_victorian,
ccimls_flex_heat_base,
ccimls_flex_heat_force_air,
ccimls_flex_heat_force_water,
ccimls_flex_heat_none,
ccimls_flex_heat_other,
ccimls_flex_pool_yes,
ccimls_flex_pool_no,
ccimls_flex_pool_unkown,
ccimls_flex_dock_yes,
ccimls_flex_dock_no,
ccimls_flex_dock_unknown,
ccimls_flex_siding_bamboard,
ccimls_flex_siding_brick,
ccimls_flex_siding_clapboard,
ccimls_flex_siding_other,
ccimls_flex_siding_shingle,
ccimls_flex_siding_stone,
ccimls_flex_siding_stucco,
ccimls_flex_siding_vertical,
ccimls_flex_siding_vinyl,
rmls_water_source_cistern,
rmls_water_source_community,
rmls_water_source_private,
rmls_water_source_public,
rmls_water_source_spring,
rmls_water_source_well,
imls_kitchen_breakfast_bar,
imls_kitchen_double_oven,
imls_kitchen_oven_range_freestanding,
imls_kitchen_oven_range_builtin,
imls_kitchen_water_softener_own,
imls_kitchen_water_softener_rent,
vbr_resiavgu_cable_tv,
vbr_resiavgu_satellite,
vbr_resiavgu_district_sewer,
vbr_resiavgu_gas,
vbr_resiavgu_snow_removal,
vbr_resiavgu_district_water,
vbr_resiavgu_phone,
vbr_resiavgu_trash_pickup,
wfrmls_style_other,
wfrmls_style_split_entry,
wfrmls_style_tri_multi_level,
wfrmls_style_townhouse_end,
wfrmls_style_townhouse_mid,
wfrmls_style_condo_main_level,
wfrmls_style_condo_mid_level,
wfrmls_style_condo_top_level,
wfrmls_style_condo_high_rise,
wfrmls_style_2_story,
wfrmls_style_condo_studio,
wfrmls_style_modular,
wfrmls_style_southwest,
wfrmls_style_aframe,
wfrmls_style_basement,
wfrmls_style_cabin,
wfrmls_style_mobile,
wfrmls_style_ranch,
wfrmls_style_twin_home,
hhimls_master_bed_first_floor,
hhimls_master_bed_second_floor,
hhimls_master_bed_third_floor,
hhimls_master_bed_fourth_floor,
hhimls_master_bed_multiple,
hhimls_master_bed_unit_level,
ebrd_solar_pool_owned,
ebrd_solar_pool_leased,
ebrd_solar_water_heater_owned,
ebrd_solar_water_heater_leased,
ebrd_solar_electrical_owned,
ebrd_solar_electrical_leased,
rarbcs_is_am_bus_route,
sandicor_roof_bitumen,
sandicor_roof_flat,
sandicor_roof_mansard,
sandicor_roof_mixed,
sandicor_roof_pitched_flat,
sandicor_roof_foam,
sandicor_roof_rubber_membrane,
sandicor_roof_shingle,
sandicor_roof_tar_gravel,
sandicor_roof_tile,
sandicor_roof_composition,
sandicor_roof_metal,
sandicor_roof_pitched,
sandicor_roof_positive_pitched,
sandicor_roof_rolled_roofing,
sandicor_roof_shake,
sandicor_roof_slate,
sandicor_roof_tar_crushed_back,
nefar_exterior_wall_aluminum_siding,
nefar_exterior_wall_brick,
nefar_exterior_wall_brick_accent,
nefar_exterior_wall_brick_front,
nefar_exterior_wall_coquina,
nefar_exterior_wall_imitation_brick,
nefar_exterior_wall_shingle_composite,
nefar_exterior_wall_stucco_coquina_front,
nefar_exterior_wall_stucco,
nefar_exterior_wall_vinyl,
nefar_exterior_wall_mostly_brick,
nefar_exterior_wall_wood_siding,
nefar_exterior_wall_metal_siding,
nefar_exterior_wall_cementious_siding,
nefar_exterior_wall_wood_shake,
nefar_exterior_wall_asbestos_siding,
nefar_exterior_wall_brick_veneer,
car_oldfather_deeded_slip,
car_oldfather_community_pool,
car_oldfather_balcony,
car_oldfather_elevator,
car_oldfather_bayfront,
car_oldfather_boat_slip,
car_oldfather_comm_55_plus,
car_oldfather_comm_marina,
car_oldfather_first_flr_master,
car_oldfather_waterview,
annual_hoa_fee_info,
annual_condo_fee_info,
annual_city_tax_info,
annual_state_county_tax_info,
peterson_master_on_main,
peterson_golf_course_community,
peterson_gated_community,
nefar_1_4_to_1_2_acre,
nefar_1_2_to_1_acre,
nefar_1_to_2_1_2_acre,
nefar_2_1_2_to_5_acre,
nefar_5_to_10_acre,
nefar_less_1_4_acre,
nefar_will_subdivide,
nefar_10_to_25_acre,
nefar_25_to_50_acre,
nefar_50_to_100_acre,
nefar_100_to_200_acre,
nefar_200_to_400_acre,
nefar_400_to_640_acre,
nefar_over_640_acre,
originating_source_info,
has_garage_4_cars,
fmls_is_1_pt_5_stories,
fmls_is_1_2_to_3_4_acres,
fmls_is_1_3_to_1_2_acre,
fmls_is_1_up_to_2_acres,
fmls_is_10_to_20_acres,
fmls_is_2_up_to_3_acres,
fmls_is_20_or_more_acres,
fmls_is_3_4_up_to_1_acre,
fmls_is_3_up_to_5_acres,
fmls_is_4_plus_car_garage,
fmls_is_5_up_to_10_acres,
fmls_is_auction,
fmls_is_brick_3_sides,
fmls_is_brick_4_sides,
fmls_is_cement_siding,
fmls_is_country_club,
fmls_is_crawl_space,
fmls_is_finished,
fmls_is_foreclosure,
fmls_is_gated,
fmls_is_golf,
fmls_is_homeowners_assoc,
fmls_is_hud_listing,
fmls_is_lake,
fmls_is_lender_owned,
fmls_is_level,
fmls_is_lot,
fmls_is_marina,
fmls_is_park,
fmls_is_playground,
fmls_is_short_sale_pre_approved,
fmls_is_side_rear_entry,
fmls_is_split_foyer,
fmls_is_split_level,
fmls_is_swimming_pool,
fmls_is_tennis_lighted,
fmls_is_under_1_3_acre,
fmls_is_unfinished,
fmls_is_vinyl_siding,
fmls_is_wooded,
gamls_is_1_2_to_1_acres,
gamls_is_1_3_to_1_2_acre,
gamls_is_1_up_to_2_acres,
gamls_is_10_to_20_acres,
gamls_is_2_up_to_5_acres,
gamls_is_20_or_more_acres,
gamls_is_5_up_to_10_acres,
gamls_is_crawl_space,
gamls_is_finished,
gamls_is_foreclosure,
gamls_is_gated,
gamls_is_homeowners_assoc,
gamls_is_lake,
gamls_is_lender_owned,
gamls_is_marina,
gamls_is_park,
gamls_is_playground,
gamls_is_short_sale_pre_approved,
gamls_is_side_rear_entry,
gamls_is_split_foyer,
gamls_is_split_level,
gamls_is_swimming_pool,
gamls_is_under_1_3_acre,
gamls_is_vinyl_siding,
TREB_is_sqft_700,
TREB_is_sqft_700_1100,
TREB_is_sqft_1100_1500,
TREB_is_sqft_1500_2000,
TREB_is_sqft_2000_2500,
TREB_is_sqft_2500_3000,
TREB_is_sqft_3000_3500,
TREB_is_sqft_3500_5000,
TREB_is_sqft_5000_plus,
rapb_no_membership_required
,is_waterfront_lake_tuscaloosa
,sandicor_o_lot
,sandicor_1_to_4k_lot
,sandicor_4k_to_7k_lot
,sandicor_7k_to_11k_lot
,sandicor_quarterac_to_halfac_lot
,sandicor_halfac_to_1ac_lot
,sandicor_1ac_to_2ac_lot
,sandicor_2ac_to_4ac_lot
,sandicor_4ac_to_10ac_lot
,sandicor_10ac_to_20ac_lot
,sandicor_20ac_plus_lot
,treb_is_sqft_0_499
,treb_is_sqft_1100_1299
,treb_is_sqft_1300_1499
,treb_is_sqft_500_699
,treb_is_sqft_700_899
,treb_is_sqft_900_1099
,armls_is_five_to_twelve_units
,has_community_boat_ramp
,has_exterior_siding
,has_exterior_wood_frame
,has_garage_6_plus
,hudmls_common_pool
,hudmls_common_tennis_court
,is_exterior_concrete_block
,is_exterior_stone
,is_exterior_stucco
,mtrmls_attached_townhouse
,mtrmls_attached_flat_condo
,mtrmls_attached_garden_condo
,mtrmls_attached_highrise_condo
,mtrmls_attached_loft_condo
,mtrmls_attached_other_condo
,mtrmls_attached_hpr_attached
,mtrmls_attached_zero_lot_line_attached
,mtrmls_detached_site_built
,mtrmls_detached_modular_home
,mtrmls_detached_manufactured_foundation
,mtrmls_detached_manufactured_mobile
,mtrmls_detached_hpr_detached


,tmls2_common_walls_1_common_wall
,tmls2_common_walls_2_plus_common_walls
,tmls2_common_walls_no_common_walls
,tmls2_common_walls_end_unit
,tmls2_common_walls_no_one_below
,tmls2_common_walls_no_one_above

,cincy_access_city_street
,cincy_access_county_road
,cincy_access_basement_road
,cincy_access_gravel_road
,cincy_access_paved_road
,cincy_access_private_drive
,cincy_access_state_highway
,cincy_access_us_highway


from stage.direct_idx_attribute_custom s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
where s.source_id in {}  and t.target_listing_id is not NULL;
"""

LISTING_ATTRIBUTE_CUSTOM_QUERY_2 = """
select 
t.source_id as source_id     ,
t.batch_id as batch_id      ,
t.target_listing_id  as listing_id   ,
t.y_creation_date      ,
y_last_update_date

,chrmls_bathroom_basement
,chrmls_bathroom_main
,chrmls_bathroom_third
,chrmls_bathroom_upper
,chrmls_bedroom_basement
,chrmls_bedroom_main
,chrmls_bedroom_third
,chrmls_bedroom_upper
,chrmls_bonus_basement
,chrmls_bonus_main
,chrmls_bonus_third
,chrmls_bonus_upper
,chrmls_den_basement
,chrmls_den_main
,chrmls_den_third
,chrmls_den_upper
,chrmls_dining_room_basement
,chrmls_dining_room_main
,chrmls_dining_room_third
,chrmls_dining_room_upper
,chrmls_kitchen_basement
,chrmls_kitchen_main
,chrmls_kitchen_third
,chrmls_kitchen_upper
,chrmls_living_room_basement
,chrmls_living_room_main
,chrmls_living_room_third
,chrmls_living_room_upper
,chrmls_master_basement
,chrmls_master_main
,chrmls_master_third
,chrmls_master_upper
,chrmls_office_basement
,chrmls_office_main
,chrmls_office_third
,chrmls_office_upper
,has_lake_name_allatoona
,has_lake_name_andrews
,has_lake_name_arrowhead
,has_lake_name_blackshear
,has_lake_name_blue_ridge
,has_lake_name_berkeley_lake
,has_lake_name_burton
,has_lake_name_carters
,has_lake_name_chatuge
,has_lake_name_chikasaw
,has_lake_name_clarkhill
,has_lake_name_eufaula
,has_lake_name_great_rock
,has_lake_name_harding
,has_lake_name_hartwell
,has_lake_name_jackson
,has_lake_name_juliette
,has_lake_name_lanier
,has_lake_name_none
,has_lake_name_norris
,has_lake_name_nottely
,has_lake_name_oconee
,has_lake_name_oliver
,has_lake_name_other
,has_lake_name_rabun
,has_lake_name_russell
,has_lake_name_seed
,has_lake_name_seminole
,has_lake_name_sinclair
,has_lake_name_swan
,has_lake_name_thurmond
,has_lake_name_tugalo
,has_lake_name_walter_f_george
,has_lake_name_weiss
,has_lake_name_westpoint
,has_lake_name_windward
,has_lake_name_worth
,has_lake_name_yonah
,hicentral_condition_excellent
,hicentral_condition_above_average
,hicentral_condition_average
,hicentral_condition_fair
,hicentral_condition_needs_major_repair
,hicentral_condition_tear_down
,chrmls2_water_body_name_lake_norman
,chrmls2_water_body_name_mountain_island_lake
,chrmls2_water_body_name_lake_wylie
,rmls_is_sqft_0_to_2999
,rmls_is_sqft_3000_to_4999
,rmls_is_sqft_5000_to_6999
,rmls_is_sqft_7000_to_9999
,rmls_is_sqft_10000_to_14999
,rmls_is_sqft_15000_to_19999
,rmls_is_sqft_20000_to_99_acres
,rmls_is_acres_1_to_3
,rmls_is_acres_3_to_5
,rmls_is_acres_5_to_7
,rmls_is_acres_7_to_10
,rmls_is_acres_10_to_20
,rmls_is_acres_20_to_50
,rmls_is_acres_50_to_99
,rmls_is_acres_100_to_200
,rmls_is_acres_200_plus
,recolorado_is_community_espadin
,mlspin_2_Family
,mlspin_3_Family
,mlspin_4_Family
,mlspin_Duplex
,mlspin_5_9_Family
,mlspin_2_Family_2_Units_Side_by_Side
,mlspin_2_Family_2_Units_Up_Down
,mlspin_2_Family_Rooming_House
,mlspin_3_Family_3_Units_Side_by_Side
,mlspin_3_Family_3_Units_Up_Down
,mlspin_3_Family_Rooming_House
,mlspin_4_Family_4_Units_Side_by_Side
,mlspin_4_Family_4_Units_Up_Down
,mlspin_4_Family_Rooming_House
,mlspin_5_plus_Family_5_plus_Units_Side_by_Side
,mlspin_5_plus_Family_5_plus_Units_Up_Down
,mlspin_5_plus_Family_Rooming_House
,ccmls_is_east_of_bus_17_lot
,ccmls_is_east_of_highway_17_bypass_lot
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Floor_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Floor_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Floor_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Floor_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Wiring_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Wiring_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Wiring_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Wiring_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Plmbg_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Plmbg_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Plmbg_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Plmbg_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_HtCool_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_HtCool_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_HtCool_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_HtCool_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Roof_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Roof_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Roof_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Roof_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Kitchen_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Kitchen_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Kitchen_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Kitchen_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Baths_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Baths_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Baths_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Baths_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Rm_Adtn_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Rm_Adtn_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Rm_Adtn_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Rm_Adtn_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Pool_Yr_Updated_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Pool_Yr_Updated_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_items_Updated_Pool_PartialFull_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_items_Updated_Pool_PartialFull_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_kitchen_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_kitchen_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_master_bath_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_master_bath_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_laundry_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_laundry_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(armls_dining_area_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as armls_dining_area_info
,armls_basement_y_n
,armls_sep_den_office_y_n
,crmls_has_vincent_thomas_bridge_view
,hmls_is_floorplan_other
,chrmls2_property_sub_type_modular
,chrmls2_property_sub_type_other
,chrmls2_lot_adjoins_nat_state_forest
,chrmls2_lot_beach_front
,chrmls2_lot_city_view
,chrmls2_lot_cleared
,chrmls2_lot_corner_lot
,chrmls2_lot_creekfront
,chrmls2_lot_vegetation_crops
,chrmls2_lot_culs_de_sac
,chrmls2_lot_flood_fringe_area
,chrmls2_lot_flood_plain_bottom_land
,chrmls2_lot_g_infill_lot
,chrmls2_lot_green_area
,chrmls2_lot_hilly
,chrmls2_lot_pond_lake
,chrmls2_lot_lake_access
,chrmls2_lot_level
,chrmls2_lot_long_range_view
,chrmls2_lot_mountain_view
,chrmls2_lot_waterfall
,chrmls2_lot_on_golf_course
,chrmls2_lot_pasture
,chrmls2_lot_paved
,chrmls2_lot_private
,chrmls2_lot_riverfront
,chrmls2_lot_rolling_slope
,chrmls2_lot_sloped
,chrmls2_lot_steep_slope
,chrmls2_lot_stream_creek
,chrmls2_lot_views
,chrmls2_lot_water_view
,chrmls2_lot_waterfront
,chrmls2_lot_wetlands
,chrmls2_lot_winter_view
,chrmls2_lot_wooded
,chrmls2_lot_year_round_view
,chrmls2_parking_attached_garage
,chrmls2_parking_back_load_garage
,chrmls2_parking_basement
,chrmls2_parking_carport_1_car
,chrmls2_parking_carport_2_car
,chrmls2_parking_carport_3_car
,chrmls2_parking_carport_4_car_plus
,chrmls2_parking_detached_garage
,chrmls2_parking_driveway
,chrmls2_parking_garage_1_car
,chrmls2_parking_garage_2_car
,chrmls2_parking_garage_3_car
,chrmls2_parking_garage_4_plus_car
,chrmls2_parking_garage_door_opener
,chrmls2_parking_golf_cart_garage
,chrmls2_parking_keypad_entry
,chrmls2_parking_parking_lot
,chrmls2_parking_on_street
,chrmls2_parking_deck
,chrmls2_parking_parking_space_1
,chrmls2_parking_parking_space_2
,chrmls2_parking_parking_space_3
,chrmls2_parking_parking_space_4_plus
,chrmls2_parking_side_load_garage
,chrmls2_parking_tandem
,chrmls2_parking_other
,chrmls2_parking_none
,chrmls2_is_waterfront_boat_house
,chrmls2_is_waterfront_boat_lift
,chrmls2_is_waterfront_boat_ramp_community
,chrmls2_is_waterfront_boat_slip_community
,chrmls2_is_waterfront_boat_slip_deed
,chrmls2_is_waterfront_boat_slip_lease_license
,chrmls2_is_waterfront_covered_structure
,chrmls2_is_waterfront_paddlesport_launch_site
,chrmls2_is_waterfront_paddlesport_launch_site_community
,chrmls2_is_waterfront_personal_watercraft_lift
,chrmls2_is_waterfront_pier
,chrmls2_is_waterfront_retaining_wall
,chrmls2_is_waterfront_other
,chrmls2_is_waterfront_none
,chrmls2_community_has_dock
,chrmls2_community_has_boat_ramp
,chrmls2_exterior_airplane_hangar
,chrmls2_exterior_arena
,chrmls2_exterior_arena_covered
,chrmls2_exterior_auto_shop
,chrmls2_exterior_barns
,chrmls2_exterior_elevator
,chrmls2_exterior_equestrian_facilities
,chrmls2_exterior_feed_barn
,chrmls2_exterior_fence
,chrmls2_exterior_fire_pit
,chrmls2_exterior_gazebo
,chrmls2_exterior_greenhouse
,chrmls2_exterior_hay_shed
,chrmls2_exterior_hot_tub
,chrmls2_exterior_gas_grill
,chrmls2_exterior_in_ground_irrigation
,chrmls2_exterior_lawn_maintenance
,chrmls2_exterior_outbuildings
,chrmls2_exterior_outdoor_fireplace
,chrmls2_exterior_outdoor_kitchen
,chrmls2_exterior_packing_shed
,chrmls2_exterior_pool_above_ground
,chrmls2_exterior_pool_in_ground
,chrmls2_exterior_porte_cochere
,chrmls2_exterior_g_rainwater_catchment
,chrmls2_exterior_rooftop_terrace
,chrmls2_exterior_satellite_internet_available
,chrmls2_exterior_sauna
,chrmls2_exterior_tractor_shed
,chrmls2_exterior_stable
,chrmls2_exterior_storage
,chrmls2_exterior_tennis_courts
,chrmls2_exterior_terrace
,chrmls2_exterior_underground_power_lines
,chrmls2_exterior_wired_internet_available
,chrmls2_exterior_workshop
,chrmls2_exterior_other
,ragfl_unit_view_bay
,ragfl_unit_view_canal
,ragfl_unit_view_club
,ragfl_unit_view_garden
,ragfl_unit_view_golf
,ragfl_unit_view_intracoastal
,ragfl_unit_view_lagoon
,ragfl_unit_view_lake
,ragfl_unit_view_ocean
,ragfl_unit_view_direct_ocean
,ragfl_unit_view_other
,ragfl_unit_view_pool
,ragfl_unit_view_river
,ragfl_unit_view_tennis
,ragfl_unit_view_water
,siar_under_1_4_acre
,siar_over_1_4_up_to_1_2_acre
,siar_over_1_2_up_to_1_acre
,siar_over_1_up_to_3_acres
,siar_over_3_up_to_6_acres
,siar_over_6_up_to_10_acres
,siar_over_10_up_to_20_acres
,siar_over_20_up_to_40_acres
,ccar_unit_location_channel_view
,ccar_unit_location_end_unit
,ccar_unit_location_golf_course_view
,ccar_unit_location_inlet_creek_view
,ccar_unit_location_island
,ccar_unit_location_lake_pond_view
,ccar_unit_location_marsh_wetlands_view
,ccar_unit_location_ocean_view
,ccar_unit_location_oceanfront_unit
,ccar_unit_location_oceanview_unit
,ccar_unit_location_penthouse
,ccar_unit_location_top_floor
,ccar_unit_location_waterway_view
,ccar_building_location_adult_community_55
,ccar_building_location_channel
,ccar_building_location_designated_flood_zone
,ccar_building_location_east_of_bus_17
,ccar_building_location_east_of_highway_17_bypass
,ccar_building_location_floating_dock
,ccar_building_location_in_golf_course_community
,ccar_building_location_in_icw_community
,ccar_building_location_inlet_creek
,ccar_building_location_inside_city_limits
,ccar_building_location_island
,ccar_building_location_marsh_view
,ccar_building_location_marsh_wetlands_view
,ccar_building_location_ocean_view
,ccar_building_location_ocean_view_lot
,ccar_building_location_oceanfront
,ccar_building_location_oceanview
,ccar_building_location_on_channel
,ccar_building_location_on_golf_course
,ccar_building_location_on_icw
,ccar_building_location_on_inlet_creek
,ccar_building_location_on_lake_pond
,ccar_building_location_on_march_wetlands
,ccar_building_location_outside_city_limits
,ccar_building_location_river
,ccar_building_location_second_row_beach
,ccar_building_location_wetlands
,treb_cnd_sqft_less_than_700
,treb_cnd_sqft_0_to_499
,treb_cnd_sqft_1100_to_1299
,treb_cnd_sqft_1100_to_1500
,treb_cnd_sqft_1300_to_1499
,treb_cnd_sqft_1500_to_2000
,treb_cnd_sqft_2000_to_2500
,treb_cnd_sqft_2500_to_3000
,treb_cnd_sqft_3000_to_3500
,treb_cnd_sqft_3500_to_5000
,treb_cnd_sqft_5000_plus
,treb_cnd_sqft_500_to_699
,treb_cnd_sqft_700_to_1100
,treb_cnd_sqft_700_to_899
,treb_cnd_sqft_900_to_1099
,siar_over_40_acres
,treb_exterior_alum_siding
,treb_exterior_board_batten
,treb_exterior_brick
,treb_exterior_brick_front
,treb_exterior_concrete
,treb_exterior_insulbrick
,treb_exterior_log
,treb_exterior_metal_side
,treb_exterior_other
,treb_exterior_shingle
,treb_exterior_stone
,treb_exterior_stucco_plaster
,treb_exterior_vinyl_siding
,treb_exterior_wood
,treb_basement_none
,treb_basement_other
,treb_basement_walk_up
,treb_garage_none
,treb_garage_other
,treb_garage_surfaced
,treb_type_com_commercial_retail
,treb_type_com_farm
,treb_type_com_industrial
,treb_type_com_investment
,treb_type_com_land
,treb_type_com_office
,treb_type_com_sale_of_business
,treb_type_com_store_w_apt_office
,treb_style_cnd_2_story
,treb_style_cnd_3_story
,treb_style_cnd_apartment
,treb_style_cnd_bachelor_studio
,treb_style_cnd_bungaloft
,treb_style_cnd_bungalow
,treb_style_cnd_industrial_loft
,treb_style_cnd_loft
,treb_style_cnd_multi_level
,treb_style_cnd_other
,treb_style_cnd_stacked_townhouse
,treb_style_cnd_warehouse_loft
,treb_cnd_type_co_op_apt
,treb_cnd_type_co_ownership_apt
,treb_cnd_type_comm_element_condo
,treb_cnd_type_condo_apt
,treb_cnd_type_condo_townhouse
,treb_cnd_type_det_condo
,treb_cnd_type_other
,treb_cnd_type_parking_space
,treb_cnd_type_room
,treb_cnd_type_semi_det_condo
,treb_cnd_type_leasehold_condo
,treb_cnd_type_locker
,treb_cnd_type_phased_condo
,treb_cnd_type_time_share
,treb_cnd_type_vacant_land_condo
,has_no_driveway
,has_other_driveway
,is_frontsplit_style
,rapb_auto_garage_open
,rapb_central_vacuum
,rapb_compactor
,rapb_cooktop
,rapb_dishwasher
,rapb_disposal
,rapb_dryer
,rapb_fire_alarm
,rapb_freezer
,rapb_gas_lease
,rapb_generator_hookup
,rapb_generator_whle_house
,rapb_hookup
,rapb_ice_maker
,rapb_intercom
,rapb_lead_cert
,rapb_microwave
,rapb_none
,rapb_purifier
,rapb_range_electric
,rapb_range_gas
,rapb_refrigerator
,rapb_reverse_osmosis_water_treatment
,rapb_satellite_dish
,rapb_smoke_detector
,rapb_solar_water_heater
,rapb_storm_shutters
,rapb_tv_antenna
,rapb_wall_oven
,rapb_washer
,rapb_washer_dryer_hookup
,rapb_water_heater_elec
,rapb_water_heater_gas
,rapb_water_softener_owned
,rapb_water_softener_rntd
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(cbmls_county_info,'None',''),', ',','),',,',',')),',')),''),',') as cbmls_county_info
,cbmls_style_attached_or_half_duplex
,cbmls_style_build_to_suit
,cbmls_style_condo
,cbmls_style_detached
,cbmls_style_manufactured_mobile_housing_land_must_convey
,cbmls_style_townhome
,cbmls_year_built
,cbmls_hoa_yes
,cbmls_hoa_no
,cbmls_school_district_agua_dulce_isd
,cbmls_school_district_alice_isd
,cbmls_school_district_aransas_pass_isd
,cbmls_school_district_banquete_isd
,cbmls_school_district_beeville_isd
,cbmls_school_district_bishop_isd
,cbmls_school_district_calallen_isd
,cbmls_school_district_corpus_christi_isd
,cbmls_school_district_driscoll_isd
,cbmls_school_district_flour_buff_isd
,cbmls_school_district_george_west_isd
,cbmls_school_district_gregory_portland_isd
,cbmls_school_district_ingleside_isd
,cbmls_school_district_kingsville_isd
,cbmls_school_district_london_isd
,cbmls_school_district_mathis_isd
,cbmls_school_district_odem_isd
,cbmls_school_district_orange_grove_isd
,cbmls_school_district_other_isd
,cbmls_school_district_padre_island_isd
,cbmls_school_district_port_aransas_isd
,cbmls_school_district_robstown
,cbmls_school_district_sinton_isd
,cbmls_school_district_tuloso_midway_isd
,cbmls_school_district_taft_isd
,cbmls_school_district_three_rivers_isd
,cbmls_school_district_west_oso_isd
,cbmls_school_district_woodsboro_isd
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(cbmls_elementary_school_info,'None',''),', ',','),',,',',')),',')),''),',') as cbmls_elementary_school_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(cbmls_middle_school_info,'None',''),', ',','),',,',',')),',')),''),',') as cbmls_middle_school_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(cbmls_high_school_info,'None',''),', ',','),',,',',')),',')),''),',') as cbmls_high_school_info
,cbmls_foreclosure_yes
,cbmls_foreclosure_no
,cbmls_fireplace_yes
,cbmls_fireplace_no
,cbmls_interior_handicap_accessible
,cbmls_interior_open_concept_floorplan
,cbmls_interior_split_bedrooms
,cbmls_swimming_pool_yes
,cbmls_swimming_pool_no
,cbmls_lot_beach_front
,cbmls_lot_beach_view
,cbmls_lot_canal
,cbmls_lot_corner
,cbmls_lot_cultivated
,cbmls_lot_golf_course
,cbmls_lot_interior
,cbmls_lot_irregular
,cbmls_lot_landscaped
,cbmls_lot_mineral_rights
,cbmls_lot_no_mineral_rights
,cbmls_lot_waterfront
,cbmls_lot_partial_mineral_rights
,cbmls_lot_partially_wooded
,cbmls_lot_water_view
,cbmls_lot_wooded
,cbmls_garage_spaces
,nalmls_age_100_plus_years
,nalmls_age_11_15_years
,nalmls_age_1_5_years
,nalmls_age_16_20_years
,nalmls_age_21_25_years
,nalmls_age_26_35_years
,nalmls_age_36_49_years
,nalmls_age_50_74_years
,nalmls_age_50_plus_years
,nalmls_age_6_10_years
,nalmls_age_75_99_years
,nalmls_age_new_construction
,nalmls_age_proposed_construction
,nalmls_age_under_construction
,glvmls_hoa_name_anthem
,glvmls_hoa_name_green_valley
,glvmls_hoa_name_green_valley_ranch
,glvmls_hoa_name_calico_ridge
,glvmls_hoa_name_macdonald_highlands
,glvmls_hoa_name_sun_city_anthem
,glvmls_hoa_name_madeira_canyon
,glvmls_hoa_name_tuscany_village
,glvmls_hoa_name_seven_hills
,glvmls_hoa_name_sun_city_mac_ranch
,glvmls_hoa_name_whitney_ranch
,wumls_has_lake_name_keowee
,michric_battle_creek_b
,michric_branch_county_r
,michric_central_michigan_c
,michric_clare_gladwin_d
,michric_eaton_county_e
,michric_grand_rapids_g
,michric_greater_kalamazoo_k
,michric_hillsdale_county_x
,michric_holland_saugatuck_h
,michric_indiana_counties_i
,michric_jackson_county_a
,michric_lenawee_county_y
,michric_masonoceanamanistee_o
,michric_montcalm_county_v
,michric_muskegon_county_m
,michric_north_ottawa_county_n
,michric_outside_michric_area_z
,michric_paul_bunyan_p
,michric_southwestern_michigan_s
,michric_st_joseph_county_j
,michric_traverse_city_t
,michric_west_central_w
,cara_transaction_type_sale
,cara_transaction_type_rent
,cara_transaction_type_sale_lease
,realcomp_zoning_agricultural
,realcomp_zoning_commercial
,realcomp_zoning_heavy_industrial
,realcomp_zoning_light_industrial
,realcomp_zoning_multi_family
,realcomp_zoning_multiple
,realcomp_zoning_office
,realcomp_zoning_other
,realcomp_zoning_recreation
,realcomp_zoning_residential
,realcomp_zoning_site_plan_condo
,	is_bayeast_status_active	,
	is_bayeast_status_back_on_market	,
	is_bayeast_status_active_contingent	,
	is_bayeast_status_new	,
	is_bayeast_status_price_change	,
	is_bayeast_status_active_reo	,
	is_bayeast_status_active_short_sale	,
	is_bayeast_status_back_on_market_reo	,
	is_bayeast_status_back_on_market_short_sale	,
	is_bayeast_status_new_reo	,
	is_bayeast_status_new_short_sale	,
	is_bayeast_status_price_change_reo	,
	is_bayeast_status_price_change_short_sale	,
	is_bayeast_status_active_coming_soon	,
	is_bayeast_status_sold	,
	is_bayeast_status_rented_leased	,
	is_bayeast_status_sold_reo	,
	is_bayeast_status_sold_short_sale	,
	is_bayeast_status_pending	,
	is_bayeast_status_pending_courtconfirmation	,
	is_bayeast_status_pending_show_for_backups	,
	is_bayeast_status_pending_subj_lenderapprov	,
	is_bayeast_status_pending_reo	,
	is_bayeast_status_pending_show_backups_reo	,
	is_bayeast_status_pending_show_backup_short	,
	is_bayeast_street_level_1_bedroom	,
	is_bayeast_street_level_2_bedrooms	,
	is_bayeast_street_level_3_bedrooms	,
	is_bayeast_street_level_4_bedrooms	,
	is_bayeast_street_level_5_bedrooms	,
	is_bayeast_street_level_6_plus_bedrooms	,
	is_bayeast_street_level_0_5_bath	,
	is_bayeast_street_level_1_bath	,
	is_bayeast_street_level_1_5_baths	,
	is_bayeast_street_level_2_baths	,
	is_bayeast_street_level_2_5_baths	,
	is_bayeast_street_level_3_baths	,
	is_bayeast_street_level_3_5_baths	,
	is_bayeast_street_level_4_baths	,
	is_bayeast_street_level_5_plus_baths	,
	is_bayeast_street_level_laundry_facility	,
	is_bayeast_street_level_main_entry	,
	is_bayeast_street_level_primary_bedrm_suite_1	,
	is_bayeast_street_level_primary_bedrm_suites_2	,
	is_bayeast_street_level_primary_bedrm_retreat	,
	is_bayeast_street_level_no_steps_to_entry	,
	is_bayeast_street_level_none	,
	is_bayeast_street_level_other
	,glvmls_1_level_1_floor
,glvmls_1_level_2_floor
,glvmls_1_level_3_floor
,glvmls_2_level
,glvmls_3_5_levels
,is_multi_level_style
,treb_is_acres__50_to_1_99
,treb_is_acres_10_to_24_99
,treb_is_acres_100_plus
,treb_is_acres_2_to_4_99
,treb_is_acres_25_to_49_99
,treb_is_acres_50_to_99_99
,treb_is_acres_less_than__49
,abor_restriction_adult_55
,abor_restriction_adult_62
,abor_restriction_building_size
,abor_restriction_building_style
,abor_restriction_city_restrictions
,abor_restriction_covenant
,abor_restriction_deed_restrictions
,abor_restriction_development_type
,abor_restriction_easement
,abor_restriction_environmental
,abor_restriction_lease
,abor_restriction_limited_vehicles
,abor_restriction_livestock
,abor_restriction_seller_imposed
,abor_restriction_zoning
,ragfl_unit_view_preserve


,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(nwmls_offers_info,'None',''),', ',','),',,',',')),',')),''),',') as nwmls_offers_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(nwmls_offers_review_date_info,'None',''),', ',','),',,',',')),',')),''),',') as nwmls_offers_review_date_info

,mlsgsc_sqft_0_to_999
,mlsgsc_sqft_1000_to_1099
,mlsgsc_sqft_1100_to_1199
,mlsgsc_sqft_1200_to_1299
,mlsgsc_sqft_1300_to_1399
,mlsgsc_sqft_1400_to_1499
,mlsgsc_sqft_1500_to_1599
,mlsgsc_sqft_1600_to_1699
,mlsgsc_sqft_1700_to_1799
,mlsgsc_sqft_1800_to_1899
,mlsgsc_sqft_1900_to_1999
,mlsgsc_sqft_2000_to_2099
,mlsgsc_sqft_2100_to_2199
,mlsgsc_sqft_2200_to_2299
,mlsgsc_sqft_2300_to_2399
,mlsgsc_sqft_2400_to_2499
,mlsgsc_sqft_2500_to_2599
,mlsgsc_sqft_2600_to_2699
,mlsgsc_sqft_2700_to_2799
,mlsgsc_sqft_2800_to_2899
,mlsgsc_sqft_2900_to_2999
,mlsgsc_sqft_3000_to_3299
,mlsgsc_sqft_3300_to_3599
,mlsgsc_sqft_3600_to_3899
,mlsgsc_sqft_3900_to_4199
,mlsgsc_sqft_4200_to_4599
,mlsgsc_sqft_5000_to_5499
,mlsgsc_sqft_5500_to_5999
,sjsrmls_condo_name
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_addn_sq_ft_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_addn_sq_ft_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_area_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_area_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_fin_bmnt_sq_ft_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_fin_bmnt_sq_ft_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_gl_prch_sq_ft_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_gl_prch_sq_ft_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_square_foot_source_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_square_foot_source_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_total_bathrooms_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_total_bathrooms_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(aaor_unfin_bmnt_sq_ft_info,'None',''),', ',','),',,',',')),',')),''),',') as aaor_unfin_bmnt_sq_ft_info
,sjsrmls_condo_name_old
,abor_has_financing_assumable
,abor_has_financing_buyer_assistance_programs
,abor_has_financing_cash
,abor_has_financing_committed_money
,abor_has_financing_contract
,abor_has_financing_conventional
,abor_has_financing_court_approval
,abor_has_financing_exchange
,abor_has_financing_fha
,abor_has_financing_fhma
,abor_has_financing_lease_back
,abor_has_financing_lease_purchase
,abor_has_financing_lender_approval
,abor_has_financing_owner_may_carry
,abor_has_financing_sba_type_loan
,abor_has_financing_see_remarks
,abor_has_financing_sell_workout
,abor_has_financing_texas_vet
,abor_has_financing_usda_loan
,abor_has_financing_va_loan
,abor_has_financing_zero_down
,glvmls_building_allure
,glvmls_building_bocaraton
,glvmls_building_casablanca
,glvmls_building_cosmopolitan
,glvmls_building_juhl
,glvmls_building_loft5
,glvmls_building_lunad
,glvmls_building_marie_antoinette
,glvmls_building_newport_lofts
,glvmls_building_one_las_vegas
,glvmls_building_one_queensridge_place
,glvmls_building_other
,glvmls_building_palms_place
,glvmls_building_panorama_towers
,glvmls_building_park_towers
,glvmls_building_platinum_hotel
,glvmls_building_regency_towers
,glvmls_building_signature_at_mgm_grand
,glvmls_building_sky_las_vegas
,glvmls_building_soho_lofts
,glvmls_building_the_martin
,glvmls_building_the_ogden
,glvmls_building_trump_international
,glvmls_building_turnberry_place
,glvmls_building_turnberry_towers
,glvmls_building_vdara_hotel_and_spa
,glvmls_building_vere_citycenter
,glvmls_building_viera
,glvmls_building_waldorf_astoria
,glvmls_building_wimbledon
,nwwmls_style_1_p_half_story_w_basement
,nwwmls_style_1_story
,nwwmls_style_1_story_w_basement
,nwwmls_style_2_story
,nwwmls_style_2_story_w_basement
,nwwmls_style_3_story
,paar_1031_exchange_financing
,paar_age_restricted
,paar_appliance_range_electric
,paar_appliance_range_gas
,paar_arizona_room_heated
,paar_assumable_financing
,paar_balloon_financing
,paar_boulders_view
,paar_bradshaw_mountain_view
,paar_cash_financing
,paar_central_air_conditioning
,paar_city_view
,paar_community_clubhouse
,paar_community_fitness_room
,paar_community_handball
,paar_community_has_game_room
,paar_community_kitchen
,paar_community_meeting_room
,paar_community_membership_included
,paar_community_membership_optional
,paar_community_membership_required
,paar_community_membership_variable
,paar_community_none
,paar_community_racquet_ball
,paar_community_restaurant
,paar_community_spa
,paar_conventional_financing
,paar_corner_lot
,paar_dark_room
,paar_electric_water_heater
,paar_exterior_brick
,paar_exterior_greenhouse
,paar_exterior_metal_frame
,paar_exterior_stucco
,paar_exterior_wood_frame
,paar_family_room
,paar_fha_qualified
,paar_fmha_rural_dev_financing
,paar_free_and_clear_financing
,paar_game_rec_room
,paar_gas_water_heater
,paar_gated_community
,paar_glassford_hill_view
,paar_golf_course_lot
,paar_golf_course_view
,paar_granite_mountain_view
,paar_great_room
,paar_handicap_accessible
,paar_handicap_bath
,paar_handicap_doorway_interior
,paar_handicap_ramp_interior
,paar_has_arizona_room_unheated
,paar_heat_forced_gas
,paar_hobby_studio_room
,paar_in_law_suites
,paar_juniper_pinon_lot
,paar_lake_view
,paar_laundry_room
,paar_lease_purchase_financing
,paar_loft
,paar_lot_boarders_golf_course
,paar_lot_boarders_national_forest
,paar_lot_boarders_state_blm
,paar_lot_creek
,paar_lot_grass
,paar_lot_has_other_trees
,paar_lot_hilltop
,paar_lot_level
,paar_lot_other
,paar_lot_plateau
,paar_lot_ponderosa_pine
,paar_lot_release_financing
,paar_lot_rolling
,paar_lot_rural
,paar_lot_view_lot
,paar_may_split_financing
,paar_media_room
,paar_membership_golf_private
,paar_membership_golf_public
,paar_mountain_view
,paar_national_forest_view
,paar_new_construction
,paar_office
,paar_on_cul_de_sac
,paar_own_bld_suit_financing
,paar_owner_lease_back_financing
,paar_owner_will_carry_terms
,paar_parking_heated_garage
,paar_parking_off_street
,paar_parking_parking_pad
,paar_parking_rv_carport
,paar_parking_rv_hookups
,paar_parking_rv_pad
,paar_parking_spaces
,paar_parking_tandem
,paar_parking_type_street
,paar_ponderosa_pine_view
,paar_pool_indoor
,paar_pool_outdoor
,paar_potential_bedroom
,paar_property_desc_hillside_lot
,paar_re_circulating_water_heater
,paar_remote_lot
,paar_room_none
,paar_rv_garage
,paar_rv_parking
,paar_sale_auction
,paar_sewer_city
,paar_sewer_septic
,paar_sf_peaks_view
,paar_sloped_gentle_lot
,paar_sloped_steep_lot
,paar_sold_furnished_financing
,paar_storage
,paar_study_den_library_room
,paar_submit_financing
,paar_tennis_community
,paar_trade_or_exchange_option
,paar_utilities_cable
,paar_utilities_electric
,paar_utilities_gas_natural
,paar_utilities_telephone
,paar_va_financing
,paar_valley_lot
,paar_view_panoramic
,paar_water_source_city
,paar_water_source_private_well
,paar_workshop
,rapb_is_acres_10_to_less_than_25
,rapb_is_acres_1_to_less_than_2
,rapb_is_acres_25_to_less_than_50
,rapb_is_acres_2_to_less_than_3
,rapb_is_acres_3_to_less_than_4
,rapb_is_acres_4_to_less_than_5
,rapb_is_acres_50_plus
,rapb_is_acres_5_to_less_than_10
,rapb_is_acres_less_than_one_fourth
,rapb_is_acres_one_fourth_to_one_half
,rapb_is_acres_one_half_to_less_than_one
,treb_is_acres_5_to_9_99
,treb_is_lot_size_code_acres
,treb_is_lot_size_code_feet
,treb_is_lot_size_code_hectares
,treb_is_lot_size_code_metres
,treb_style_com_agricultural
,treb_style_com_apartment
,treb_style_com_automotive
,treb_style_com_commercial
,treb_style_com_duplex
,treb_style_com_fourplex
,treb_style_com_general
,treb_style_com_highway_commercial
,treb_style_com_industrial
,treb_style_com_institutional
,treb_style_com_mixed
,treb_style_com_multiplex
,treb_style_com_multi_unit
,treb_style_com_multi_use
,treb_style_com_office
,treb_style_com_recreational
,treb_style_com_residential
,treb_style_com_restaurant
,treb_style_com_retail
,treb_style_com_store_with_apt_office
,treb_style_com_triplex
,treb_style_com_warehouse
,treb_style_com_with_property
,treb_use_com_apt_hotel
,treb_use_com_apts_13_to_20_units
,treb_use_com_apts_2_to_5_units
,treb_use_com_apts_6_to_12_units
,treb_use_com_apts_over_20_units
,treb_use_com_automotive_related
,treb_use_com_auto_wreckers
,treb_use_com_banquet_hall
,treb_use_com_building
,treb_use_com_building_supplies
,treb_use_com_car_wash
,treb_use_com_church
,treb_use_com_clinic
,treb_use_com_day_care
,treb_use_com_development_site
,treb_use_com_drugstore_pharmacy
,treb_use_com_funeral_home
,treb_use_com_gas_plus_convenience
,treb_use_com_gas_plus_service
,treb_use_com_gas_stations
,treb_use_com_golf
,treb_use_com_golf_course
,treb_use_com_golf_driving_range
,treb_use_com_gravel_pit_quarry
,treb_use_com_grocery_supermarket
,treb_use_com_health_fitness_club
,treb_use_com_hospitality
,treb_use_com_hospitality_food_related
,treb_use_com_hostel
,treb_use_com_hotel_motel_inn
,treb_use_com_hotel_tavern
,treb_use_com_industrial
,treb_use_com_industrial_park
,treb_use_com_lots
,treb_use_com_mall
,treb_use_com_manufacturing
,treb_use_com_manufacturing_warehouse
,treb_use_com_mixed
,treb_use_com_mixed_2_or_more_uses
,treb_use_com_mixed_complex
,treb_use_com_mixed_use_farm
,treb_use_com_motel
,treb_use_com_muffler_shop
,treb_use_com_multiple_2_or_more_units
,treb_use_com_multi_unit_building
,treb_use_com_nursery_plants
,treb_use_com_office
,treb_use_com_office_complex
,treb_use_com_office_condo
,treb_use_com_office_equipment
,treb_use_com_office_secretarial
,treb_use_com_offices_residential
,treb_use_com_offices_retail
,treb_use_com_other_automotive
,treb_use_com_other_farms
,treb_use_com_other_land
,treb_use_com_other_recreation
,treb_use_com_parking_lot
,treb_use_com_plaza
,treb_use_com_plaza_apartments
,treb_use_com_plaza_offices
,treb_use_com_real_estate_office
,treb_use_com_recreational
,treb_use_com_recreation_centre
,treb_use_com_residential
,treb_use_com_resort
,treb_use_com_restaurant
,treb_use_com_retail
,treb_use_com_schools
,treb_use_com_self_storage
,treb_use_com_seniors_residence
,treb_use_com_shopping_centre
,treb_use_com_shopping_centre_condo
,treb_use_com_shopping_ctr_kiosk
,treb_use_com_special_care_homes
,treb_use_com_townhouse
,treb_use_com_townhouse_complex
,treb_use_com_warehousing
,treb_use_com_waterfront
,treb_use_com_water_frontage
,treb_use_com_woodworking
,nwamls_water_body_name
,triad_sqft_range

,abor_fema_flood_plain_no
,abor_fema_flood_plain_partial
,abor_fema_flood_plain_see_remarks
,abor_fema_flood_plain_yes_100_yr
,abor_fema_flood_plain_yes_500_yr

,akmls_access_type_floatplane
,akmls_access_type_gravel
,akmls_access_type_paved
,akmls_access_type_private
,akmls_access_type_trail
,akmls_access_type_water
,akmls_access_type_maintained
,akmls_access_type_unmaintained
,akmls_access_type_government
,akmls_access_type_unknown_btv
,glarmls_has_brick				
,glarmls_has_heat_pump
,glarmls_has_minisplit_ductless
,glarmls_has_electric
,glarmls_has_golf_course
,glarmls_has_hot_tub
,glarmls_has_sauna_steam
,glarmls_has_none
,glarmls_has_other
,glarmls_has_propane
,glarmls_has_well_water
,glarmls_closets_level_1
,glarmls_closets_level_2
,glarmls_closets_level_3
,glarmls_has_aluminum
,glarmls_has_block
,glarmls_fireplaces_level_2
,glarmls_fireplaces_level_3
,glarmls_has_construction_brick
,glarmls_has_brk_ven
,glarmls_has_cement_board
,glarmls_has_frame_wood
,glarmls_has_log
,glarmls_has_other_na
,glarmls_has_construction_stone
,glarmls_has_stone_veneer
,glarmls_has_stucco
,glarmls_has_sythentic_stucco
,glarmls_has_vinyl
,glarmls_of_hvac_units
,glarmls_has_central_air
,glarmls_has_cooling_heat_pump
,glarmls_has_concrete_blk
,glarmls_has_cooling_minisplit_ductless
,glarmls_has_cooling_none
,glarmls_lake_pond
,glarmls_pasture_acres
,glarmls_tillable_acres
,glarmls_timber_acres
,glarmls_1st_floor_bedrooms
,glarmls_2nd_floor_bedrooms
,glarmls_basement_bedrooms
,glarmls_upper_floor_bedrooms
,glarmls_above_grade_finished
,glarmls_above_grade_unfin
,glarmls_acres
,glarmls_age
,glarmls_below_grade_finished
,glarmls_below_grade_unfin
,glarmls_garage_spaces
,glarmls_nonconform_sqft_fin
,glarmls_nonconform_sqft_uf
,glarmls_sqft_total_unfin
,glarmls_sqft_total_finished
,glarmls_stories
,glarmls_total_bedrooms
,glarmls_total_of_rooms
,glarmls_total_baths
,glarmls_total_closets
,glarmls_total_fireplaces
,glarmls_year_built
,glarmls_of_garage_spaces
,glarmls_of_surface_parking_spaces
,glarmls_has_cellar
,glarmls_has_finished
,glarmls_has_outside_entry
,glarmls_has_partially_finished
,glarmls_has_unfinished
,glarmls_has_walk_up
,glarmls_has_walkout_finished
,glarmls_has_walkout_part_fin
,glarmls_has_walkout_unfinished
,glarmls_has_wall_window_unit_s
,glarmls_has_balcony
,glarmls_has_boat_slip
,glarmls_has_creek
,glarmls_has_deck
,glarmls_has_handic_prov
,glarmls_has_lake
,glarmls_has_out_buildings
,glarmls_has_patio
,glarmls_has_pond
,glarmls_has_pool_above_ground
,glarmls_has_pool_in_ground
,glarmls_has_porch
,glarmls_has_screened_in_porch
,glarmls_has_tennis_court_s
,glarmls_has_waterfront
,glarmls_has_barn_util
,glarmls_has_equipment
,glarmls_has_irrigation_system
,glarmls_has_livestock
,glarmls_has_silo_grain
,glarmls_has_stable
,glarmls_has_tobacco_barn
,glarmls_has_chain_link
,glarmls_has_farm
,glarmls_has_full
,glarmls_has_partial
,glarmls_has_privacy
,glarmls_has_splitrail
,glarmls_has_wood
,glarmls_has_crawl_space
,glarmls_has_poured_concrete
,glarmls_has_slab
,glarmls_has_1_car_carport
,glarmls_has_2_car_carport
,glarmls_has_3_car_carport
,glarmls_has_attached
,glarmls_has_detached
,glarmls_has_driveway
,glarmls_has_electric_vehicle_charging_station_s
,glarmls_has_entry_front
,glarmls_has_entry_rear
,glarmls_has_entry_side
,glarmls_has_lower_level
,glarmls_has_off_street_parking
,glarmls_has_street
,glarmls_has_basement_y_n
,glarmls_has_first_floor_laundry
,glarmls_has_first_floor_pbr
,glarmls_has_garage_y_n
,glarmls_has_improvements_sold_as_is
,glarmls_has_geothermal
,glarmls_has_spray_in_foam_insulation
,glarmls_has_solar_panel
,glarmls_has_tankless_water_heater
,glarmls_has_forced_air
,glarmls_has_gravity
,glarmls_has_natural_gas
,glarmls_has_radiant
,glarmls_has_steam
,glarmls_has_cable_tv
,glarmls_has_exterior_maint
,glarmls_has_gas
,glarmls_has_groundskeeping
,glarmls_has_heat
,glarmls_has_internet
,glarmls_has_mstr_ins
,glarmls_has_security
,glarmls_has_sewer
,glarmls_has_snow_removal
,glarmls_has_trash
,glarmls_has_water
,glarmls_has_addlndave
,glarmls_has_cleared
,glarmls_has_corner
,glarmls_has_covt_restr
,glarmls_has_cul_de_sac
,glarmls_has_deadend
,glarmls_has_easement
,glarmls_has_flood_insurance_req
,glarmls_has_irregular
,glarmls_has_level
,glarmls_has_sidewalk
,glarmls_has_storm_sewer
,glarmls_has_will_divide
,glarmls_has_wooded
,glarmls_has_zero_lot_line
,glarmls_has_flat
,glarmls_has_metal
,glarmls_has_rubber
,glarmls_has_shingle
,glarmls_has_slate
,glarmls_has_tile
,glarmls_has_bedroom
,glarmls_has_breakfast_room
,glarmls_has_craft_hobby_room
,glarmls_has_den
,glarmls_has_dining_area
,glarmls_has_dining_room
,glarmls_has_exercise_room
,glarmls_has_family_room
,glarmls_has_florida_room
,glarmls_has_foyer
,glarmls_has_full_bathroom
,glarmls_has_game_room
,glarmls_has_great_room
,glarmls_has_gym
,glarmls_has_half_bathroom
,glarmls_has_kitchen
,glarmls_has_laundry
,glarmls_has_library
,glarmls_has_living_room
,glarmls_has_loft
,glarmls_has_media_room
,glarmls_has_mud_room
,glarmls_has_office
,glarmls_has_primary_bathroom
,glarmls_has_primary_bedroom
,glarmls_has_sauna
,glarmls_has_separate_apartment
,glarmls_has_sitting_room
,glarmls_has_study
,glarmls_has_additional_strg
,glarmls_has_clubhouse
,glarmls_has_dock
,glarmls_has_elevator
,glarmls_has_fitness_room
,glarmls_has_gated_community
,glarmls_has_guest_room
,glarmls_has_hoa_first_right_of_refusal
,glarmls_has_laundry_facility
,glarmls_has_laundry_located_in_unit
,glarmls_has_marina
,glarmls_has_pets_allowed_per_restrictions
,glarmls_has_playground
,glarmls_has_pool
,glarmls_has_rental_allowed
,glarmls_has_secured_bldg
,glarmls_has_tennis_court
,glarmls_has_cistern_water
,glarmls_has_electricity_connected
,glarmls_has_fuel_natural
,glarmls_has_public_sewer
,glarmls_has_public_water
,glarmls_has_septic_system
,glarmls_has_basement_none
,glarmls_closets_basement
,glarmls_has_cooling_other
,glarmls_has_exterior_hot_tub
,glarmls_has_exterior_sauna_steam
,glarmls_features_has_well_water
,glarmls_has_fendcing_electric
,glarmls_has_fencing_none
,glarmls_has_fencing_stone
,glarmls_fireplaces_level_1
,glarmls_has_foundation_other
,glarmls_has_parking_none
,glarmls_has_heating_electric
,glarmls_has_heating_other
,glarmls_has_heating_propane
,glarmls_has_lot_golf_course
,glarmls_has_roof_other
,glarmls_assumable
,glarmls_baths_1_2
,glarmls_baths_full
,glarmls_laundry_level
,glarmls_monthly_maintenance
,glarmls_bedroom_level
,glarmls_breakfast_room_level
,glarmls_craft_hobby_room_level
,glarmls_den_level
,glarmls_dining_area_level
,glarmls_dining_room_level
,glarmls_exercise_room_level
,glarmls_family_room_level
,glarmls_florida_room_level
,glarmls_foyer_level
,glarmls_full_bathroom_level
,glarmls_game_room_level
,glarmls_great_room_level
,glarmls_gym_level
,glarmls_half_bathroom_level
,glarmls_kitchen_level
,glarmls_library_level
,glarmls_living_room_level
,glarmls_loft_level
,glarmls_media_room_level
,glarmls_mud_room_level
,glarmls_office_level
,glarmls_primary_bathroom_level
,glarmls_primary_bedroom_level
,glarmls_sauna_level
,glarmls_separate_apartment_level
,glarmls_sitting_room_level
,glarmls_study_level
,glarmls_m_struct_flood_plain
,glarmls_style
,glarmls_fha_approved_as_of
,glarmls_location_of_parking
,glarmls_year_building_built
,glarmls_year_unit_finished
,glarmls_fireplaces_basement

------------------
,abor_restriction_none
,creb_restrictions_adult_living   
,creb_restrictions_architectural_guidelines   
,creb_restrictions_airspace_restriction   
,creb_restrictions_building_commitment_time_to_start   
,creb_restrictions_biological_restrictions   
,creb_restrictions_board_approval   
,creb_restrictions_building_design_size   
,creb_restrictions_building_restriction   
,creb_restrictions_children   
,creb_restrictions_condo_strata_approval   
,creb_restrictions_covenant_road_restriction   
,creb_restrictions_development_restriction   
,creb_restrictions_easement_registered_on_title   
,creb_restrictions_elevator_access_restriction   
,creb_restrictions_encroachment   
,creb_restrictions_encumbrance   
,creb_restrictions_environmental_restriction   
,creb_restrictions_floor_space_ratio   
,creb_restrictions_historic_site   
,creb_restrictions_lease_restriction   
,creb_restrictions_special_licensing_required   
,creb_restrictions_call_lister   
,creb_restrictions_landlord_approval   
,creb_restrictions_long_range_transport   
,creb_restrictions_leased_equipment_assumed   
,creb_restrictions_mineral_claim_staked   
,creb_restrictions_mineral_rights   
,creb_restrictions_mandatory_building_scheme   
,creb_restrictions_nature_conservancy   
,creb_restrictions_none_known   
,creb_restrictions_noise_restriction   
,creb_restrictions_pets_not_allowed   
,creb_restrictions_non_smoking_building   
,creb_restrictions_overhead_right_of_way   
,creb_restrictions_pets_allowed   
,creb_restrictions_pet_restrictions_or_board_approval_required   
,creb_restrictions_phone_listing_broker   
,creb_restrictions_park_approval   
,creb_restrictions_restrictive_covenant_building_design_size   
,creb_restrictions_subject_to_final_registration   
,creb_restrictions_see_remarks   
,creb_restrictions_rental   
,creb_restrictions_restrictive_covenant   
,creb_restrictions_must_remove_mobile_home_from_park   
,creb_restrictions_road_access_agreement   
,creb_restrictions_restrictive_use_clause   
,creb_restrictions_right_of_way_non_reg   
,creb_restrictions_short_term_rentals_not_allowed   
,creb_restrictions_short_term_rentals_allowed   
,creb_restrictions_subject_to_final_subdivision_approval   
,creb_restrictions_surface_right_of_way   
,creb_restrictions_third_party_right_of_way   
,creb_restrictions_tree_preservation   
,creb_restrictions_underground_utility_right_of_way   
,creb_restrictions_utility_right_of_way

,fmls_lower_level_bedrooms
,fmls_lower_level_full_bathrooms
,fmls_lower_level_half_bathrooms
,fmls_upper_level_bedrooms
,fmls_upper_level_full_bathrooms
,fmls_upper_level_half_bathrooms
,fmls_main_level_bedrooms
,fmls_main_level_bathrooms
,fmls_main_level_half_bathrooms
,btvar_land_permitted_horses
,btvar_land_permitted_livestock
,btvar_land_permitted_manufactured_sp_home
,btvar_land_permitted_modular
,btvar_land_permitted_other
,btvar_land_permitted_poultry
,btvar_land_permitted_see_sp_remarks
,btvar_land_permitted_site_sp_built
,scwmls_has_water_municipal_water
,scwmls_has_water_municipal_sewer
,scwmls_has_water_well
,scwmls_has_water_joint_well
,scwmls_has_water_non_municipal_prvt_dispos
,scwmls_has_water_holding_tank
,scwmls_has_water_no_sewer
,scwmls_has_water_no_water
,scwmls_has_water_other
,scwmls_has_water_community_well
,scwmls_has_water_sand_point_well
,scwmls_has_water_mound_system
,harmls_acres_00_to_25
,harmls_acres_1_to_2
,harmls_acres_10_to_15
, harmls_acres_1_to_3
, harmls_acres_20_to_50
, harmls_acres_25_to_50
, harmls_acres_2_to_5
, harmls_acres_3_to_5
, harmls_acres_50_to_100
, harmls_acres_5_to_10
,harmls_acres_75_to_100
,harmls_acres__50
,harmls_acres_15_to_20
,harmls_acres_50_to_300
,rapb_boat_services_private_dock  
,rapb_boat_services_common_dock  
,rapb_boat_services_up_to_20_ft_boat  
,rapb_boat_services_up_to_30_ft_boat  
,rapb_boat_services_up_to_40_ft_boat  
,rapb_boat_services_up_to_50_ft_boat  
,rapb_boat_services_up_to_60_ft_boat  
,rapb_boat_services_up_to_70_ft_boat  
,rapb_boat_services_up_to_80_ft_boat  
,rapb_boat_services_up_to_90_ft_boat  
,rapb_boat_services_up_to_100_ft_boat  
,rapb_boat_services_over_101_ft_boat  
,rapb_boat_services_lift  
,rapb_boat_services_hoist_davit  
,rapb_boat_services_boathouse  
,rapb_boat_services_electric_available  
,rapb_boat_services_water_available  
,rapb_boat_services_fuel  
,rapb_boat_services_marina  
,rapb_boat_services_yacht_club  
,rapb_boat_services_attended  
,rapb_boat_services_full_service  
,rapb_boat_services_restroom  
,rapb_boat_services_sew_pump_available  
,rapb_boat_services_subject_to_lease  
,rapb_boat_services_overnight  
,rapb_boat_services_live_aboard  
,rapb_boat_services_parking  
,rapb_boat_services_ramp  
,rapb_boat_services_boat_lock  
,rapb_boat_services_exclusive_use  
,rapb_boat_services_wake_zone  
,rapb_boat_services_no_wake_zone
,ranw_condo_1_story
,ranw_condo_2_story
,ranw_condo_conversion
,ranw_condo_free_standing
,ranw_condo_mid_rise
,ranw_condo_side_by_side
,ranw_garage_1_5_car
,ranw_garage_1_car
,ranw_garage_2_5_car
,ranw_garage_2_car
,ranw_garage_3_3_5_car
,ranw_garage_4_car

from stage.direct_idx_attribute_custom_2 s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
where s.source_id in {}  and t.target_listing_id is not NULL;
"""

LISTING_ATTRIBUTE_CUSTOM_QUERY_3 = """
select 
t.source_id as source_id     ,
t.batch_id as batch_id      ,
t.target_listing_id   as listing_id  ,
t.y_creation_date      ,
y_last_update_date,

realcomp_is_waterfront_all_sports_lake,
ntreis_is_highland_homes_brokerage,

libor_hoa_inc_heat,
libor_hoa_inc_exterior_maintenance,
libor_hoa_inc_other,
libor_hoa_inc_air_conditioning,
libor_hoa_inc_cable_tv,
libor_hoa_inc_air_conditioning_allowed,
libor_hoa_inc_electricity,
libor_hoa_inc_housekeeping,
libor_hoa_inc_water,
libor_hoa_inc_trash,
libor_hoa_inc_pool_care,
libor_hoa_inc_sewer,
libor_hoa_inc_gas,
libor_hoa_inc_hot_water,
libor_hoa_inc_maintenance_grounds,
libor_hoa_inc_snow_removal,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_electric_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_electric_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_exterior_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_exterior_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_fencing_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_fencing_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_financial_details_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_financial_details_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_fireplace_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_fireplace_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_flooring_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_flooring_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_foundation_details_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_foundation_details_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_fuel_cost_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_fuel_cost_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_green_energy_efficient_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_green_energy_efficient_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_green_energy_generation_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_green_energy_generation_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_green_indoor_air_quality_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_green_indoor_air_quality_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_green_sustainability_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_green_sustainability_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_green_verification_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_green_verification_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_green_water_conservation_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_green_water_conservation_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_heating_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_heating_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_interior_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_interior_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_laundry_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_laundry_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_listing_terms_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_listing_terms_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_lot_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_lot_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_other_equipment_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_other_equipment_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_other_structures_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_other_structures_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_owner_pays_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_owner_pays_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_parking_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_parking_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_patio_and_porch_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_patio_and_porch_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_pool_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_pool_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_possession_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_possession_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_power_production_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_power_production_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_road_frontage_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_road_frontage_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_roof_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_roof_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_security_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_security_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_separate_utilities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_separate_utilities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_sewer_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_sewer_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_showing_requirements_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_showing_requirements_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_special_listing_conditions_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_special_listing_conditions_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_tenant_pays_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_tenant_pays_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_amenities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_appliances_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_appliances_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_dining_room_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_dining_room_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_amenities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_appliances_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_appliances_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_dining_room_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_dining_room_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_amenities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_appliances_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_appliances_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_dining_room_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_dining_room_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_amenities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_appliances_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_appliances_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_dining_room_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_dining_room_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_5_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_5_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_5_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_5_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_6_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_6_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_6_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_6_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_7_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_7_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_7_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_7_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_8_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_8_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_8_owner_furnish_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_8_owner_furnish_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_utilities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_utilities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_view_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_view_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_water_source_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_water_source_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_waterfront_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_waterfront_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_window_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_window_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_information_number_of_stoves_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_information_number_of_stoves_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_information_number_of_washers_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_information_number_of_washers_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_information_number_of_air_conditoners_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_information_number_of_air_conditoners_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_information_number_of_dryers_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_information_number_of_dryers_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_information_number_of_refrigerators_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_information_number_of_refrigerators_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_information_number_of_dishwashers_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_information_number_of_dishwashers_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_monthly_rent_mf_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_monthly_rent_mf_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_rent_desc_mf_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_rent_desc_mf_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_lease_term_mf_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_lease_term_mf_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_total_rooms_mf_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_total_rooms_mf_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_1_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_1_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_monthly_rent_mf_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_monthly_rent_mf_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_rent_desc_mf_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_rent_desc_mf_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_lease_term_mf_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_lease_term_mf_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_total_rooms_mf_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_total_rooms_mf_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_2_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_2_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_monthly_rent_mf_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_monthly_rent_mf_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_rent_desc_mf_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_rent_desc_mf_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_lease_term_mf_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_lease_term_mf_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_total_rooms_mf_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_total_rooms_mf_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_3_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_3_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_monthly_rent_mf_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_monthly_rent_mf_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_rent_desc_mf_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_rent_desc_mf_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_lease_term_mf_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_lease_term_mf_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_total_rooms_mf_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_total_rooms_mf_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_4_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_4_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_monthly_rent_mf_unit_5_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_monthly_rent_mf_unit_5_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_rent_desc_mf_unit_5_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_rent_desc_mf_unit_5_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_lease_term_mf_unit_5_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_lease_term_mf_unit_5_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_total_rooms_mf_unit_5_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_total_rooms_mf_unit_5_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_5_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_5_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_5_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_5_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_monthly_rent_mf_unit_6_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_monthly_rent_mf_unit_6_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_rent_desc_mf_unit_6_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_rent_desc_mf_unit_6_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_lease_term_mf_unit_6_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_lease_term_mf_unit_6_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_total_rooms_mf_unit_6_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_total_rooms_mf_unit_6_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_6_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_6_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_6_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_6_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_monthly_rent_mf_unit_7_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_monthly_rent_mf_unit_7_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_rent_desc_mf_unit_7_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_rent_desc_mf_unit_7_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_lease_term_mf_unit_7_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_lease_term_mf_unit_7_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_total_rooms_mf_unit_7_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_total_rooms_mf_unit_7_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_7_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_7_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_7_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_7_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_unit_location_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_unit_location_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_unit_available_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_unit_available_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_unit_occupancy_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_unit_occupancy_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_occupant_type_mf_unit_	,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_occupant_type_mf_unit_	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_monthly_rent_mf_unit_8_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_monthly_rent_mf_unit_8_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_rent_desc_mf_unit_8_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_rent_desc_mf_unit_8_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_lease_term_mf_unit_8_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_lease_term_mf_unit_8_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_total_rooms_mf_unit_8_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_total_rooms_mf_unit_8_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_total_bedrooms_mf_unit_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_total_bedrooms_mf_unit_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_total_full_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_total_full_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_multi_family_unit_8_detail_total_half_baths_mf_un_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_multi_family_unit_8_detail_total_half_baths_mf_un_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_8_owner_furnish_features_owner_furnishes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_8_owner_furnish_features_owner_furnishes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_rent_description_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_rent_description_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_rent_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_rent_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_lease_term_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_lease_term_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_occupant_type_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_occupant_type_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_kitchen_unit_1_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_kitchen_unit_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_total_bedrooms_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_total_bedrooms_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_bedrooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_bedrooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_bedrooms_on_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_bedrooms_on_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_bedrooms_on_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_bedrooms_on_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_bedrooms_on_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_bedrooms_on_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_total_full_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_total_full_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_full_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_full_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_full_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_full_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_full_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_full_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_full_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_full_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_total_half_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_total_half_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_partial_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_partial_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_partial_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_partial_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_partial_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_partial_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_partial_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_partial_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_dining_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_dining_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_dining_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_dining_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_dining_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_dining_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_dining_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_dining_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_kitchens_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_kitchens_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_kitchens_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_kitchens_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_kitchens_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_kitchens_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_kitchens_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_kitchens_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_living_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_living_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_living_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_living_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_living_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_living_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_living_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_living_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_other_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_other_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_other_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_other_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_other_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_other_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_1_detail_other_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_1_detail_other_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_rent_description_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_rent_description_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_rent_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_rent_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_lease_term_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_lease_term_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_occupant_type_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_occupant_type_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_kitchen_unit_2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_kitchen_unit_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_total_bedrooms_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_total_bedrooms_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_bedrooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_bedrooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_bedrooms_on_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_bedrooms_on_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_bedrooms_on_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_bedrooms_on_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_bedrooms_on_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_bedrooms_on_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_total_full_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_total_full_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_full_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_full_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_full_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_full_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_full_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_full_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_full_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_full_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_total_half_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_total_half_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_partial_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_partial_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_partial_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_partial_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_partial_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_partial_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_partial_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_partial_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_dining_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_dining_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_dining_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_dining_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_dining_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_dining_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_dining_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_dining_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_kitchens_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_kitchens_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_kitchens_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_kitchens_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_kitchens_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_kitchens_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_kitchens_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_kitchens_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_living_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_living_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_living_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_living_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_living_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_living_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_living_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_living_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_other_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_other_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_other_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_other_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_other_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_other_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_2_detail_other_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_2_detail_other_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_rent_description_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_rent_description_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_rent_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_rent_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_lease_term_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_lease_term_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_occupant_type_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_occupant_type_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_kitchen_unit_3_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_kitchen_unit_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_total_bedrooms_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_total_bedrooms_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_bedrooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_bedrooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_bedrooms_on_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_bedrooms_on_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_bedrooms_on_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_bedrooms_on_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_bedrooms_on_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_bedrooms_on_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_total_full_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_total_full_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_full_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_full_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_full_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_full_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_full_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_full_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_full_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_full_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_total_half_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_total_half_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_partial_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_partial_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_partial_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_partial_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_partial_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_partial_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_partial_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_partial_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_dining_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_dining_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_dining_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_dining_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_dining_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_dining_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_dining_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_dining_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_kitchens_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_kitchens_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_kitchens_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_kitchens_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_kitchens_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_kitchens_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_kitchens_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_kitchens_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_living_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_living_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_living_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_living_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_living_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_living_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_living_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_living_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_other_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_other_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_other_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_other_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_other_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_other_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_3_detail_other_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_3_detail_other_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_rent_description_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_rent_description_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_rent_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_rent_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_lease_term_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_lease_term_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_occupant_type_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_occupant_type_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_kitchen_unit_4_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_kitchen_unit_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_total_bedrooms_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_total_bedrooms_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_bedrooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_bedrooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_bedrooms_on_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_bedrooms_on_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_bedrooms_on_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_bedrooms_on_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_bedrooms_on_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_bedrooms_on_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_total_full_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_total_full_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_full_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_full_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_full_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_full_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_full_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_full_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_full_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_full_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_total_half_baths_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_total_half_baths_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_partial_baths_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_partial_baths_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_partial_baths_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_partial_baths_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_partial_baths_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_partial_baths_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_partial_baths_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_partial_baths_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_dining_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_dining_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_dining_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_dining_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_dining_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_dining_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_dining_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_dining_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_kitchens_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_kitchens_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_kitchens_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_kitchens_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_kitchens_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_kitchens_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_kitchens_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_kitchens_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_living_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_living_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_living_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_living_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_living_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_living_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_living_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_living_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_other_rooms_in_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_other_rooms_in_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_other_rooms_1st_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_other_rooms_1st_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_other_rooms_2nd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_other_rooms_2nd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_unit_4_detail_other_rooms_3rd_floor_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_unit_4_detail_other_rooms_3rd_floor_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_rental_income_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_rental_income_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_rental_income_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_rental_income_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_other_income_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_other_income_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_other_income_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_other_income_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_total_income_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_total_income_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_electric_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_electric_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_electric_expense_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_electric_expense_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_gas_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_gas_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_gas_expense_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_gas_expense_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_insurance_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_insurance_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_insurance_expense_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_insurance_expense_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_miscellaneous_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_miscellaneous_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_miscellaneous_expense_descrip_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_miscellaneous_expense_descrip_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_oil_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_oil_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_oil_expense_description_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_oil_expense_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_water_sewer_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_water_sewer_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_water_sewer_expense_desc_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_water_sewer_expense_desc_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_fuel_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_fuel_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_heat_expense_est_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_heat_expense_est_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_maintenance_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_maintenance_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_operating_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_operating_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_net_operating_income_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_net_operating_income_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_manager_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_manager_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_other_expense_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_other_expense_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_income_and_expenses_total_income_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_income_and_expenses_total_income_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_accessibility_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_accessibility_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_architectural_style_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_architectural_style_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_association_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_association_amenities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_association_fee_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_association_fee_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_attic_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_attic_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_construction_materials_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_construction_materials_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_cooling_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_cooling_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_documents_available_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_documents_available_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_door_features_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_door_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(mf_duplex_type_info,'None',''),', ',','),',,',',')),',')),''),',') as 	mf_duplex_type_info,


is_ctmls_seasonal_property,
ctmls_sq_ft_est_heated_above_grade,
ctmls_sq_ft_est_heated_below_grade,
has_ctmls_appliance_allowance,
has_ctmls_appliance_electric_dryer,
has_ctmls_appliance_gas_dryer,
has_ctmls_appliance_ice_maker,
has_ctmls_appliance_none,
has_ctmls_appliance_oven_range,
has_ctmls_appliance_range_hood,
has_ctmls_appliance_subzero,
has_ctmls_appliance_wall_oven,
has_ctmls_appliance_wine_chiller,
has_ctmls_breezeway,
has_ctmls_built_green_certification,
has_ctmls_built_in_audio,
has_ctmls_cable_prewired,
has_ctmls_cooling_split_system,
has_ctmls_electric_outlet,
has_ctmls_employee_lounge,
has_ctmls_entertainment_system,
has_ctmls_extra_insulation,
has_ctmls_fire_suppression_system,
has_ctmls_freight_elevator,
has_ctmls_generator_ready,
has_ctmls_heat_gas_in_street,
has_ctmls_heat_gas_on_gas,
has_ctmls_heat_warm_air_retired,
has_ctmls_heat_wood_coal_stove,
has_ctmls_hoists,
has_ctmls_home_automation_appliances,
has_ctmls_home_energy_rating,
has_ctmls_hot_water_100_gallon_tank,
has_ctmls_hot_water_30_gallon_tank,
has_ctmls_hot_water_40_gallon_tank,
has_ctmls_hot_water_50_gallon_tank,
has_ctmls_hot_water_65_gallon_tank,
has_ctmls_hot_water_80_gallon_tank,
has_ctmls_hot_water_domestic,
has_ctmls_hot_water_electric,
has_ctmls_hot_water_natural_gas,
has_ctmls_hot_water_oil,
has_ctmls_hot_water_other,
has_ctmls_hot_water_propane,
has_ctmls_hot_water_solar_assisted,
has_ctmls_humidistat,
has_ctmls_in_law_apartment,
has_ctmls_interior_none,
has_ctmls_kitchen_cooktop,
has_ctmls_lighting,
has_ctmls_living_space_available,
has_ctmls_loading_dock_height,
has_ctmls_loading_grade,
has_ctmls_loading_rail_height,
has_ctmls_loading_waterfront,
has_ctmls_locks,
has_ctmls_mud_room,
has_ctmls_music_room,
has_ctmls_no_hot_water,
has_ctmls_no_in_law_apartment,
has_ctmls_pool_alarm,
has_ctmls_pool_house,
has_ctmls_pool_infinity_edge,
has_ctmls_pool_power_lift,
has_ctmls_pool_ramp_entrance,
has_ctmls_pool_slide,
has_ctmls_pool_tile,
has_ctmls_possible_in_law_apartment,
has_ctmls_programmable_thermostat,
has_ctmls_public_restrooms,
has_ctmls_ridge_vents,
has_ctmls_roughed_in_bath,
has_ctmls_sauna,
has_ctmls_sewer_septic_required,
has_ctmls_sewer_shared_septic,
has_ctmls_solarium,
has_ctmls_thermopane_windows,
has_ctmls_thermostats,
has_ctmls_window_display,
has_ctmls_wired_for_audio,
is_ctmls_assessments_no_special_association_assessments,
is_ctmls_assessments_special_association_assessments,
is_ctmls_attic_access_via_hatch,
is_ctmls_attic_crawl_space,
is_ctmls_attic_finished,
is_ctmls_attic_floored,
is_ctmls_attic_heated,
is_ctmls_attic_partially_finished,
is_ctmls_attic_pull_down_stairs,
is_ctmls_attic_storage_space,
is_ctmls_attic_walk_up,
is_ctmls_basement_cooled,
is_ctmls_basement_full_with_hatchway,
is_ctmls_basement_full_with_walkout,
is_ctmls_basement_hatchway_access,
is_ctmls_basement_heated,
is_ctmls_basement_liveable_space,
is_ctmls_basement_partial_with_hatchway,
is_ctmls_basement_partial_with_walkout,
is_ctmls_basement_shared_basement,
is_ctmls_exterior_awnings,
is_ctmls_exterior_breezeway,
is_ctmls_exterior_door_sign,
is_ctmls_exterior_doors_10_to_15_ft,
is_ctmls_exterior_doors_16_to_20_ft,
is_ctmls_exterior_doors_20_plus_ft,
is_ctmls_exterior_doors_under_10_ft,
is_ctmls_exterior_fruit_trees,
is_ctmls_exterior_gutters,
is_ctmls_exterior_incinerator,
is_ctmls_exterior_kennel,
is_ctmls_exterior_levelers,
is_ctmls_exterior_loading_dock_grade,
is_ctmls_exterior_loading_dock_well,
is_ctmls_exterior_none,
is_ctmls_exterior_paddock,
is_ctmls_exterior_pole_sign,
is_ctmls_exterior_porch_enclosed,
is_ctmls_exterior_porch_heated,
is_ctmls_exterior_porch_wrap_around,
is_ctmls_exterior_roof_sign,
is_ctmls_exterior_shed,
is_ctmls_exterior_stone_wall,
is_ctmls_exterior_underground_sprinkler,
is_ctmls_exterior_wrap_around_deck,
is_ctmls_first_floorl_laundry,
is_ctmls_fuel_tank_above_ground,
is_ctmls_fuel_tank_in_basement,
is_ctmls_fuel_tank_in_garage,
is_ctmls_fuel_tank_in_ground,
is_ctmls_fuel_tank_non_applicable,
is_ctmls_laundry_room_with_sink_and_srorage,
is_ctmls_second_laundry,
is_ctmls_siding_asbestos,
is_ctmls_siding_block,
is_ctmls_siding_brick,
is_ctmls_siding_cedar,
is_ctmls_siding_cement_block,
is_ctmls_siding_clapboard,
is_ctmls_siding_hardie_board,
is_ctmls_siding_logs,
is_ctmls_siding_metal,
is_ctmls_siding_poured_concrete,
is_ctmls_siding_redwood,
is_ctmls_siding_shake,
is_ctmls_siding_shingle,
is_ctmls_siding_steel_siding,
is_ctmls_siding_stone,
is_ctmls_siding_stucco,
is_ctmls_siding_vertical_siding,
is_ctmls_undeclared_short_sale,
is_ctmls_washer_dryer_laundry,
has_ctmls_audio_system,
hhimls_not_sun_city_area,

ncrmls_island_location,
ncrmls_mainlaind_location,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_2_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_2_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_2_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_2_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_3_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_3_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_3_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_3_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_4_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_4_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_4_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_4_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_6_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_6_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_bedroom_6_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_bedroom_6_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_total_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_total_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_unit_1s_number_of_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_unit_1s_number_of_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_unit_2s_number_of_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_unit_2s_number_of_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_unit_3s_number_of_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_unit_3s_number_of_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_unit_4s_number_of_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_unit_4s_number_of_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_unit_5s_number_of_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_unit_5s_number_of_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_unit_6s_number_of_bedrooms_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_unit_6s_number_of_bedrooms_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_dining_room_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_dining_room_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_dining_room_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_dining_room_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_preformatted_display_address_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_preformatted_display_address_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_energy_star_year_certified_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_energy_star_year_certified_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_family_room_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_family_room_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_family_room_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_family_room_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_association_fee_includes_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_association_fee_includes_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_basement_foundation_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_basement_foundation_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_common_amenities_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_common_amenities_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_construction_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_construction_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_cooling_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_cooling_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_property_data_available_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_property_data_available_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_design_features_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_design_features_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_development_status_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_development_status_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_disabled_accessibility_features_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_disabled_accessibility_features_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_energy_features_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_energy_features_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_fireplaces_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_fireplaces_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_heating_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_heating_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_horse_property_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_horse_property_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_inclusions_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_inclusions_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_irrigation_type_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_irrigation_type_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_land_size_range_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_land_size_range_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_land_type_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_land_type_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_large_animals_allowed_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_large_animals_allowed_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_location_description_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_location_description_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_lot_improvements_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_lot_improvements_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_master_bedroom_bath_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_master_bedroom_bath_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_mineral_water_rights_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_mineral_water_rights_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_new_financing_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_new_financing_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_number_of_living_units_allowed_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_number_of_living_units_allowed_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_outbuildings_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_outbuildings_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_outdoor_features_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_outdoor_features_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_ownership_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_ownership_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_parking_per_unit_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_parking_per_unit_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_possible_usage_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_possible_usage_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_property_features_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_property_features_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_road_access_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_road_access_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_property_styles_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_property_styles_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_tenant_pays_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_tenant_pays_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_utilities_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_utilities_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_property_views_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_property_views_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_number_of_garage_spaces_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_number_of_garage_spaces_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_garage_type_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_garage_type_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_great_room_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_great_room_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_great_room_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_great_room_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_grossoperatingincome_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_grossoperatingincome_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_gross_scheduled_income_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_gross_scheduled_income_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_hoa2_fee_amount_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_hoa2_fee_amount_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_hoa_fee_amount_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_hoa_fee_amount_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_hoa_fee_frequency_code_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_hoa_fee_frequency_code_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_kitchen_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_kitchen_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_kitchen_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_kitchen_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_laundry_room_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_laundry_room_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_laundry_room_level_code_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_laundry_room_level_code_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_laundry_room_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_laundry_room_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_leed_year_certified_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_leed_year_certified_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_listing_url_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_listing_url_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_living_room_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_living_room_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_living_room_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_living_room_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_lot_size_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_lot_size_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_master_bedroom_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_master_bedroom_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_master_bedroom_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_master_bedroom_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_nahb_year_certified_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_nahb_year_certified_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_net_operating_income_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_net_operating_income_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_new_construction_status_code_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_new_construction_status_code_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_rec_room_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_rec_room_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_rec_room_width_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_rec_room_width_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_solar_pv_kilowatts_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_solar_pv_kilowatts_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_solar_pv_year_installed_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_solar_pv_year_installed_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_solar_thermal_type_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_solar_thermal_type_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_solar_thermal_year_installed_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_solar_thermal_year_installed_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_study_length_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_study_length_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_subdivision_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_subdivision_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_taxes_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_taxes_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_tax_year_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_tax_year_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_total_number_of_units_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_total_number_of_units_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_total_operating_epense_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_total_operating_epense_info	,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(iresmls_zoning_info ,'None',''),', ',','),',,',',')),',')),''),',') as 	iresmls_zoning_info	,

naar_restriction_architecture_committee,
naar_restriction_builder_restriction,
naar_restriction_horses_livestock_allowed,
naar_restriction_mandatory_owners_assoc,
naar_restriction_none,
naar_restriction_other,
naar_restriction_other_bldg_restrictions,
naar_restriction_other_covenants,
naar_restriction_pets_breed_restriction,
naar_restriction_pets_cats_allowed,
naar_restriction_pets_dogs_allowed,
naar_restriction_pets_number_limit,
naar_restriction_pets_weight_height_limit,
naar_restriction_pets_not_allowed,
naar_restriction_rental_restrictions_may_apply,
naar_restriction_rentals_not_permitted,
naar_restriction_seniors_55,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(glvmls_distance_to_electric_info ,'None',''),', ',','),',,',',')),',')),''),',') as   glvmls_distance_to_electric_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(glvmls_distance_to_water_info ,'None',''),', ',','),',,',',')),',')),''),',') as  glvmls_distance_to_water_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(glvmls_distance_to_sewer_info ,'None',''),', ',','),',,',',')),',')),''),',') as  glvmls_distance_to_sewer_info,

akmls_zoning_r_recreational,
akmls_zoning_r_residential,
akmls_zoning_r1_residential,
akmls_zoning_r1_single_family_or_duplex,
akmls_zoning_r1_single_family_res_city_palmer_wasilla,
akmls_zoning_r1_single_family_residential,
akmls_zoning_r2_double_family,
akmls_zoning_r2_low_density_residential,
akmls_zoning_r2_low_density_residential_city_palmer,
akmls_zoning_r2_multi_family,
akmls_zoning_r2_residential,
akmls_zoning_r2_residential_city_wasilla,
akmls_zoning_r2a_two_family_residential,
akmls_zoning_r2d_two_family_residential,
akmls_zoning_r2m_multi_family_residential,
akmls_zoning_r2mhp_multi_family_and_mobile_home,
akmls_zoning_r3_medium_density_multi_family,
akmls_zoning_r3_medium_density_multi_family_city_palmer,
akmls_zoning_r3_multi_family,
akmls_zoning_r3_multi_family_residential,
akmls_zoning_r3_residential,
akmls_zoning_r4_high_density_res_district,
akmls_zoning_r4_high_density_res_district_city_palmer,

is_bcar_direct_bay_front,
is_bcar_direct_bay_across_rd,
is_bcar_indirect_bay_side,
is_bcar_indirect_bay_across_rd,
is_bcar_direct_bayou_front,
is_bcar_direct_bayou_across_rd,
is_bcar_indirect_bayou_side,
is_bcar_indirect_bayou_across_rd,
is_bcar_direct_gulf_front,
is_bcar_direct_gulf_across_rd,
is_bcar_indirect_gulf_side,
is_bcar_indirect_gulf_across_rd,
is_bcar_direct_icw_front,
is_bcar_direct_icw_across_rd,
is_bcar_indirect_icw_side,
is_bcar_indirect_icw_across_rd,
is_bcar_direct_lagoon_front,
is_bcar_direct_lagoon_across_rd,
is_bcar_indirect_lagoon_side,
is_bcar_indirect_lagoon_across_rd,
is_bcar_direct_lake_front,
is_bcar_direct_lake_across_rd,
is_bcar_indirect_lake_side,
is_bcar_indirect_lake_across_rd,
is_bcar_direct_river_front,
is_bcar_direct_river_across_rd,
is_bcar_indirect_river_side,
is_bcar_indirect_river_across_rd,
is_is_bcar_view_canal,
is_bcar_eastern_view,
is_bcar_golf_course_view,
is_bcar_marina_view,
is_bcar_view_none_not_applicable,
is_bcar_northern_view,
is_bcar_view_other,
is_bcar_view_park,
is_bcar_pool_area_view,
is_bcar_view_skyline,
is_bcar_southern_view,
is_bcar_western_view,
is_bcar_view_wooded,

michric_allen_edwin_realty ,

has_utilities_dixie_power,


harmls_disclosure_special_addendum,
harmls_disclosure_corporate_listing,
harmls_disclosure_covenants_conditions_restrictions,
harmls_disclosure_estate,
harmls_disclosure_exclusions,
harmls_disclosure_real_estate_owned,
harmls_disclosure_home_protection_plan,
harmls_disclosure_levee_district,
harmls_disclosure_mi_lenders_approval,
harmls_disclosure_mud,
harmls_disclosure_no_disclosures,
harmls_disclosure_non_refundable_application_fee,
harmls_disclosure_other_disclosures,
harmls_disclosure_owner_agent,
harmls_disclosure_pets,
harmls_disclosure_pre_foreclosure,
harmls_disclosure_probate,
harmls_disclosure_reports_available,
harmls_disclosure_hoa_first_right_of_refusal,
harmls_disclosure_sellers_disclosure,
harmls_disclosure_short_sale,
harmls_disclosure_approved_seniors_project,
harmls_disclosure_tenant_occupied,
vreb_ownership_co_op,
vreb_ownership_freehold,
vreb_ownership_freehold_strata,
vreb_ownership_fractional_ownership,
vreb_ownership_hotel_strata,
vreb_ownership_leasehold,
vreb_ownership_leasehold_strata,
vreb_ownership_pad_rental,
vreb_ownership_see_supplements,
vreb_ownership_other,
vreb_ownership_strata,
vreb_sub_type_single_family_detached,
vreb_sub_type_half_duplex,
vreb_sub_type_condo_apartment,
vreb_sub_type_row_townhouse,
vreb_sub_type_full_duplex,
vreb_sub_type_triplex,
vreb_sub_type_quadruplex,
vreb_sub_type_manufactured_home,
vreb_sub_type_recreational,
vreb_sub_type_land,
vreb_sub_type_other,


string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_1_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_1_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_1_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_1_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_1_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_1_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_2_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_2_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_2_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_2_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_2_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_2_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_3_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_3_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_3_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_3_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_3_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_3_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_4_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_4_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_4_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_4_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_4_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_4_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_4_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_4_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_5_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_5_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_5_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_5_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_5_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_5_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_5_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_5_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_6_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_6_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_6_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_6_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_6_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_6_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_6_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_6_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_7_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_7_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_7_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_7_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_7_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_7_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_7_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_7_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_8_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_8_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_8_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_8_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_8_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_8_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_8_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_8_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_9_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_9_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_9_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_9_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_9_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_9_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_9_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_9_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_10_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_10_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_10_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_10_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_10_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_10_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_10_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_10_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_11_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_11_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_11_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_11_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_11_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_11_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_11_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_11_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_12_desc_1_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_12_desc_1_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_12_desc_2_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_12_desc_2_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_12_desc_3_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_12_desc_3_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(treb_room_12_info ,'None',''),', ',','),',,',',')),',')),''),',') as treb_room_12_info,
treb_room_1_length_info,
treb_room_1_width_info,
treb_room_2_length_info,
treb_room_2_width_info,
treb_room_3_length_info,
treb_room_3_width_info,
treb_room_4_length_info,
treb_room_4_width_info,
treb_room_5_length_info,
treb_room_5_width_info,
treb_room_6_length_info,
treb_room_6_width_info,
treb_room_7_length_info,
treb_room_7_width_info,
treb_room_8_length_info,
treb_room_8_width_info,
treb_room_9_length_info,
treb_room_9_width_info,
treb_room_10_length_info,
treb_room_10_width_info,
treb_room_11_length_info,
treb_room_11_width_info,
treb_room_12_length_info,
treb_room_12_width_info,
cren_has_waterfront_creek,
cren_has_waterfront_lake_reservoir,
cren_has_waterfront_other,
cren_has_waterfront_pond,
cren_has_waterfront_river,
cren_has_waterfront_seasonal_stream_spring,
cren_property_desc_adj_to_greenbelt,
cren_property_desc_adj_to_open_space,
cren_property_desc_boarders_golf_course,
cren_property_desc_boarders_national_forest,
cren_property_desc_boarders_public_land,
cren_property_desc_boundaries_marked,
cren_property_desc_boundaries_surveyed,
cren_property_desc_cleared,
cren_property_desc_corner,
cren_property_desc_corners_marked,
cren_property_desc_cul_de_sac,
cren_property_desc_foothills,
cren_property_desc_golf_course_near,
cren_property_desc_off_grid,
cren_property_desc_other,
cren_property_desc_pasture,
cren_property_desc_wooded_lot,
cren_property_desc_ski_in_ski_out,
cren_property_desc_skier_access,
cren_is_cattle_allowed,
cren_is_horses_allowed,
cren_is_other_allowed,
cren_is_poultry_allowed,
cren_is_sheep_allowed,
cren_is_swine_allowed,
cren_has_water_source_augmented_well,
cren_has_water_source_central_water,
cren_has_water_source_cistern,
cren_has_water_source_city_water,
cren_has_water_source_domestic_well,
cren_has_water_source_in_house_well,
cren_has_water_source_installed_paid,
cren_has_water_source_other,
cren_has_water_source_public,
cren_has_water_source_shared_well_spring,
cren_has_water_source_spring,
cren_has_water_source_well_other,

treb_is_exposure_all,
treb_is_exposure_e,
treb_is_exposure_n,
treb_is_exposure_ne,
treb_is_exposure_nw,
treb_is_exposure_s,
treb_is_exposure_se,
treb_is_exposure_sw,
treb_is_exposure_w,

naar_outbuilding_additional_garage,
naar_outbuilding_barns,
naar_outbuilding_boat_house,
naar_outbuilding_bunk_house,
naar_outbuilding_chicken_coop_barn,
naar_outbuilding_dog_kennel,
naar_outbuilding_gazebo,
naar_outbuilding_grain_bin,
naar_outbuilding_granary,
naar_outbuilding_greenhouse,
naar_outbuilding_guest_house,
naar_outbuilding_hen_house,
naar_outbuilding_hog_house,
naar_outbuilding_hot_tub,
naar_outbuilding_indoor_arena,
naar_outbuilding_lean_to,
naar_outbuilding_loafing_shed,
naar_outbuilding_machine_shed,
naar_outbuilding_meat_shed,
naar_outbuilding_milk_house,
naar_outbuilding_other,
naar_outbuilding_outdoor_arena,
naar_outbuilding_pole_building,
naar_outbuilding_root_cellar,
naar_outbuilding_sauna,
naar_outbuilding_screenhouse,
naar_outbuilding_second_residence,
naar_outbuilding_silo,
naar_outbuilding_stables,
naar_outbuilding_storage_shed,
naar_outbuilding_studio,
naar_outbuilding_tack_room,
naar_outbuilding_workshop,
naar_sewer_aerator,
naar_sewer_city_sewer_in_street,
naar_sewer_city_sewer_connected,
naar_sewer_compost,
naar_sewer_holding_tank,
naar_sewer_lagoon,
naar_sewer_mound_septic,
naar_sewer_outhouse,
naar_sewer_septic_system_compliant_no,
naar_sewer_septic_system_compliant_yes,
naar_sewer_shared_septic,
naar_sewer_tank_with_drainage_field,
naar_zoning_agriculture,
naar_zoning_business_commercial,
naar_zoning_forestry,
naar_zoning_other,
naar_zoning_residential_multi_family,
naar_zoning_residential_single_family,
naar_zoning_shoreline,
is_divvy_listing,

string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_bedrooms_total_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_bedrooms_total_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_bathrooms_total_integer_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_bathrooms_total_integer_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_bathrooms_full_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_bathrooms_full_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_bathrooms_half_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_bathrooms_half_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_basement_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_basement_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_heating_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_heating_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_cooling_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_cooling_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_appliances_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_appliances_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_window_features_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_window_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_interior_features_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_interior_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_building_area_total_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_building_area_total_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_attic_description_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_attic_description_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_parking_total_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_parking_total_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_parking_features_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_parking_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_levels_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_levels_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_stories_total_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_stories_total_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_patio_and_porch_features_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_patio_and_porch_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_lot_size_area_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_lot_size_area_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_lot_features_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_lot_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_parcel_number_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_parcel_number_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_inclusions_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_inclusions_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_construction_materials_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_construction_materials_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_property_condition_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_property_condition_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_year_built_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_year_built_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_sewer_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_sewer_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_water_source_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_water_source_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_utilities_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_utilities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_green_building_verification_type_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_green_building_verification_type_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_building_location_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_building_location_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_association_amenities_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_association_amenities_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_association_fee_includes_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_association_fee_includes_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_hot_water_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_hot_water_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_garbage_removal_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_garbage_removal_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_exterior_features_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_exterior_features_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_tax_annual_amount_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_tax_annual_amount_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_tax_block_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_tax_block_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_tax_lot_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_tax_lot_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_tax_map_number_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_tax_map_number_info,
string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(libor_tax_other_annual_assessment_amount_info,'None',''),', ',','),',,',',')),',')),''),',') as libor_tax_other_annual_assessment_amount_info

,	rebgv_comm_sub_space_sqft_range
,	rebgv_comm_transaction_type_lease
,	rebgv_comm_transaction_type_sale



, rebgv_comm_major_business_real_estate_services
, rebgv_comm_major_business_automotive
, rebgv_comm_prop_type_retail
, rebgv_comm_major_business_food_and_beverage
, rebgv_comm_prop_type_office
, rebgv_comm_major_business_personal_services
, rebgv_comm_prop_type_land_commercial
, rebgv_comm_prop_type_multi_family_commercial
, rebgv_comm_major_business_computing_electronics_audio_video
, rebgv_comm_major_business_sports_and_recreation
, rebgv_comm_major_business_manufacturing
, rebgv_comm_major_business_healthcare_sales_services_supplies
, rebgv_comm_major_business_general_retail
, rebgv_comm_prop_type_industrial
, rebgv_comm_major_business_accomodation_travel_and_tourism
, rebgv_comm_major_business_transportation_and_warehousing
, rebgv_comm_prop_type_business
, rebgv_comm_major_business_arts_and_entertainment
, rebgv_comm_major_business_professional_administrative_services
, rebgv_comm_major_business_marine_sales_services_supplies
, rebgv_comm_major_business_animal_production_farms
, rebgv_comm_major_business_educational_services


,	his_lava_zone_1
,	his_lava_zone_2
,	his_lava_zone_3
,	his_lava_zone_4
,	his_lava_zone_5
,	his_lava_zone_6
,	his_lava_zone_7
,	his_lava_zone_8
,	his_lava_zone_9
,	hicentral_zoning_00_residential
,	hicentral_zoning_02_r_20_residential_district
,	hicentral_zoning_03_r10_residential_district
,	hicentral_zoning_04_r_7_5_residential_district
,	hicentral_zoning_05_r_5_residential_district
,	hicentral_zoning_07_r_3_5_residential_district
,	hicentral_zoning_11_a_1_low_density_apartment
,	hicentral_zoning_12_a_2_medium_density_apartme
,	hicentral_zoning_13_a_3_high_density_apartment
,	hicentral_zoning_16_amx_1_low_density_apt_mixe
,	hicentral_zoning_17_amx_2_medium_density_apt_m
,	hicentral_zoning_18_amx_3_high_density_apt_mix
,	hicentral_zoning_20_resort
,	hicentral_zoning_21_resort_district
,	hicentral_zoning_30_commercial
,	hicentral_zoning_31_b_1_neighborhood_business
,	hicentral_zoning_32_b_2_community_business_dis
,	hicentral_zoning_33_bmx_3_community_business_m
,	hicentral_zoning_34_bmx_4_central_business_mix
,	hicentral_zoning_40_industrial
,	hicentral_zoning_41_i_1_limited_industrial_dis
,	hicentral_zoning_42_i_2_general_industrial_dis
,	hicentral_zoning_43_i_3_waterfront_industrial
,	hicentral_zoning_44_i_4_waterfront_industrial
,	hicentral_zoning_46_imx_1_industrial_commercia
,	hicentral_zoning_50_agricultural
,	hicentral_zoning_51_ag_1_restricted_agricultur
,	hicentral_zoning_52_ag_2_general_agricultural
,	hicentral_zoning_56_country_district
,	hicentral_zoning_60_preservation
,	hicentral_zoning_61_p_1_restricted_preservatio
,	hicentral_zoning_62_p_2_general_preservation
,	hicentral_zoning_63_military_federal_preserv
,	hicentral_zoning_70_public
,	hicentral_zoning_80_military
,	hicentral_zoning_90_parks_recreation
,	hicentral_zoning_99_see_amended_dp_map
,	hicentral_zoning_a_5a_agricultural_district
,	hicentral_zoning_am_apartment_mixed_use
,	hicentral_zoning_bm_business_mixed_use
,	hicentral_zoning_im_industrial_mixed_use
,	hicentral_zoning_ka_state_jurisdiction__refer
,	hicentral_zoning_kak_kakaako_community_development_project
,	hicentral_zoning_kc_state_jurisdiction__refer
,	hicentral_zoning_x2_apartment_precinct
,	hicentral_zoning_x5_resort_commercial_precinct
,	hicentral_zoning_x6_resort_mixed_use_precinct
,	hicentral_zoning_x7_public_precinct
,	hicentral_zoning_x9_marine_precinct
,	rebgv_is_bylaws_age_restrictions
,	rebgv_is_bylaws_no_restrictions
,	rebgv_is_bylaws_pets_allowed
,	rebgv_is_bylaws_pets_allowed_w_rest
,	rebgv_is_bylaws_pets_not_allowed
,	rebgv_is_bylaws_rentals_allowed
,	rebgv_is_bylaws_rentals_allwd_w_restrctns
,	rebgv_is_bylaws_rentals_not_allowed
,	rebgv_is_bylaws_smoking_restrictions
,	rebgv_is_construction_brick
,	rebgv_is_construction_concrete
,	rebgv_is_construction_concrete_block
,	rebgv_is_construction_concrete_frame
,	rebgv_is_construction_not_concrete
,	rebgv_is_construction_frame_metal
,	rebgv_is_construction_frame_wood
,	rebgv_is_construction_not_wood
,	rebgv_is_construction_log
,	rebgv_is_construction_manufactured_mobile
,	rebgv_is_construction_modular_prefab
,	rebgv_is_construction_other
,	rebgv_units_in_development
,	rebgv_is_court_ordered_sale

,	armls_current_financing_assume_no_qualify
,	armls_current_financing_assume_qualify
,	armls_current_financing_fha
,	armls_current_financing_non_assumable
,	armls_current_financing_va
,	armls_current_financing_wrap
,	armls_current_financing_all_asm_exist_no_qlf
,	armls_current_financing_assumeno_qualify
,	armls_current_financing_assumequalify
,	armls_current_financing_ballooncall_prvisn
,	armls_current_financing_fin_info_sub_to_veri
,	armls_current_financing_interest_only
,	armls_current_financing_no_prepay_penalty
,	armls_current_financing_not_applicable
,	armls_current_financing_assumable
,	armls_current_financing_balloons
,	armls_current_financing_chattel
,	armls_current_financing_conventional
,	armls_current_financing_fin_info_subj_verify
,	armls_current_financing_ida
,	armls_current_financing_leasehold
,	armls_current_financing_nonassumable
,	armls_current_financing_other
,	armls_current_financing_private
,	armls_current_financing_sba_loan
,	armls_current_financing_see_remarks
,	armls_current_financing_treat_as_free_clr
,	armls_current_financing_unsecured
,bright_transportation_airport_less_than_10_miles
,bright_transportation_bus_stop_less_than_1_mile
,bright_transportation_metro_subway_station_less_than_1_mile
,bright_transportation_metro_subway_station_1_to_3_miles
,bright_transportation_commuter_lots_less_than_5_miles
,bright_transportation_commuter_rail_station_less_than_1_mile
,bright_transportation_commuter_rail_station_1_to_5_miles
,tmls_home_builder_eci
,tmls_agent_sharon_evans
,bright_fin_bank_portfolio,
 bright_fin_assumption,
 bright_fin_cash,
 bright_fin_contract,
 bright_fin_conventional,
 bright_fin_exchange,
 bright_fin_farm_credit_service,
 bright_fin_fha,
 bright_fin_fha_203_b,
 bright_fin_fha_203_k,
 bright_fin_fha_energy_efficient_mortgage_qualified,
 bright_fin_fhlmc,
 bright_fin_fhva,
 bright_fin_fmha,
 bright_fin_fnma,
 bright_fin_industrial_development_authority,
 bright_fin_installment_sale,
 bright_fin_joint_venture,
 bright_fin_lease_purchase,
 bright_fin_negotiable,
 bright_fin_other,
 bright_fin_phfa,
 bright_fin_private,
 bright_fin_rural_development,
 bright_fin_seller_financing,
 bright_fin_state_gi_loan,
 bright_fin_usda,
 bright_fin_va,
 bright_fin_variable,
 bright_fin_vhda,
 bright_fin_wrap
 
,abor_heating_active_solar
,abor_heating_baseboard
,abor_heating_blr
,abor_heating_ceiling
,abor_heating_central
,abor_heating_coal
,abor_heating_coal_stove
,abor_heating_ductless
,abor_heating_electric
,abor_heating_energy_star_qualified_equipment
,abor_heating_energy_star_acca_rsi_quality_install
,abor_heating_exhaust_fan
,abor_heating_fireplace_insert
,abor_heating_fireplace_s
,abor_heating_floor_furnace
,abor_heating_forced_air
,abor_heating_geothermal
,abor_heating_gravity
,abor_heating_heat_pump
,abor_heating_hot_water
,abor_heating_humidity_control
,abor_heating_kerosene
,abor_heating_natural_gas
,abor_heating_none
,abor_heating_oil
,abor_heating_passive_solar
,abor_heating_pellet_stove
,abor_heating_propane
,abor_heating_propane_stove
,abor_heating_radiant
,abor_heating_radiant_ceiling
,abor_heating_radiant_floor
,abor_heating_see_remarks
,abor_heating_separate_meters
,abor_heating_space_heater
,abor_heating_steam
,abor_heating_varies_by_unit
,abor_heating_wall_furnace
,abor_heating_wood
,abor_heating_wood_stove
,abor_heating_zoned
,abor_cooling_attic_fan
,abor_cooling_ceiling_fan_s
,abor_cooling_central_air
,abor_cooling_dual
,abor_cooling_ductless
,abor_cooling_electric
,abor_cooling_energy_star_qualified_equipment
,abor_cooling_evaporative_cooling
,abor_cooling_exhaust_fan
,abor_cooling_gas
,abor_cooling_geothermal
,abor_cooling_heat_pump
,abor_cooling_humidity_control
,abor_cooling_mini_split_system
,abor_cooling_multi_units
,abor_cooling_none
,abor_cooling_roof_turbine_s
,abor_cooling_see_remarks
,abor_cooling_separate_meters
,abor_cooling_varies_by_unit
,abor_cooling_wall_window_unit_s
,abor_cooling_whole_house_fan
,abor_cooling_zoned
,abor_sewer_aerobic_septic
,abor_sewer_cesspool
,abor_sewer_engineered_septic
,abor_sewer_holding_tank
,abor_sewer_mound_septic
,abor_sewer_mud
,abor_sewer_municipal_utility_district_mud
,abor_sewer_none
,abor_sewer_perc_test_on_file
,abor_sewer_perc_test_required
,abor_sewer_private_sewer
,abor_sewer_public_sewer
,abor_sewer_see_remarks
,abor_sewer_septic_needed
,abor_sewer_septic_shared
,abor_sewer_septic_tank
,abor_utilities_above_ground
,abor_utilities_cable_available
,abor_utilities_cable_connected
,abor_utilities_cable_not_available
,abor_utilities_electricity_available
,abor_utilities_electricity_connected
,abor_utilities_electricity_not_available
,abor_utilities_internet_cable
,abor_utilities_internet_fiber
,abor_utilities_natural_gas_available
,abor_utilities_natural_gas_connected
,abor_utilities_natural_gas_not_available
,abor_utilities_none
,abor_utilities_none_available
,abor_utilities_other
,abor_utilities_phone_available
,abor_utilities_phone_connected
,abor_utilities_phone_not_available
,abor_utilities_propane
,abor_utilities_propane_needed
,abor_utilities_see_remarks
,abor_utilities_sewer_available
,abor_utilities_sewer_connected
,abor_utilities_sewer_not_available
,abor_utilities_solar
,abor_utilities_underground_utilities
,abor_utilities_water_available
,abor_utilities_water_connected
,abor_utilities_water_not_available
,abor_utilities_wind
,abor_watersource_cistern
,abor_watersource_mud
,abor_watersource_municipal_utility_district_mud
,abor_watersource_none
,abor_watersource_private
,abor_watersource_public
,abor_watersource_see_remarks
,abor_watersource_shared_well
,abor_watersource_spring
,abor_watersource_water_line_available
,abor_watersource_water_line_on_the_property
,abor_watersource_well
,abor_separate_utilities 
,abor_separate_kit_facilities
,abor_see_remarks
,abor_none
,abor_separate_entrance
,abor_room_w_priv_bath
,abor_main_level
,abor_not_connected
,abor_guest_house
,abor_connected
,abor_garage_apartment
,abor_separate_living_quart
,chrmls_lot_cleared
,chrmls_lot_adjoins_forest
,chrmls_lot_beach_front
,chrmls_lot_corner_lot
,chrmls_lot_creek_front
,chrmls_lot_creek_stream
,chrmls_lot_crops
,chrmls_lot_cul_de_sac
,chrmls_lot_end_unit
,chrmls_lot_flood_fringe_area
,chrmls_lot_flood_plain_bottom_land
,chrmls_lot_green_area
,chrmls_lot_hilly
,chrmls_lot_infill_lot
,chrmls_lot_lake_on_property
,chrmls_lot_level
,chrmls_lot_on_golf_course
,chrmls_lot_open_lot
,chrmls_lot_orchard
,chrmls_lot_other_see_remarks
,chrmls_lot_pasture
,chrmls_lot_paved
,chrmls_lot_pond
,chrmls_lot_private
,chrmls_lot_river_front
,chrmls_lot_rolling_slope
,chrmls_lot_runway_lot
,chrmls_lot_sloped
,chrmls_lot_steep_slope
,chrmls_lot_taxiway_lot
,chrmls_lot_views
,chrmls_lot_waterfall_artificial
,chrmls_lot_waterfall
,chrmls_lot_waterfront
,chrmls_lot_wetlands
,chrmls_lot_wooded
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_land_lease_expiration_date_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_land_lease_expiration_date_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_land_lease_y_n_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_land_lease_y_n_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_leasable_area_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_leasable_area_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_leasable_area_units_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_leasable_area_units_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_lease_assignable_y_n_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_lease_assignable_y_n_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_lease_considered_y_n_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_lease_considered_y_n_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_lease_expiration_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_lease_expiration_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_lease_renewal_option_y_n_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_lease_renewal_option_y_n_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_lease_term_info,'None',''),', ',','),',,',',')),',')),''),',') as 	ncrmls_lease_term_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_info_is_property_sub_to_vacation_rental_act2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_info_is_property_sub_to_vacation_rental_act2_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_info_is_property_sub_to_vacation_rental_act_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_info_is_property_sub_to_vacation_rental_act_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_info_is_the_seller_the_property_manager2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_info_is_the_seller_the_property_manager2_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_info_is_the_seller_the_property_manager_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_info_is_the_seller_the_property_manager_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_accepts_subsidized_rent_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_accepts_subsidized_rent_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_application_fee2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_application_fee2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_check_credit_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_check_credit_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_contact_information2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_contact_information2_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_contact_information_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_contact_information_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_gross_income_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_gross_income_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_is_property_subject_to_lease2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_is_property_subject_to_lease2_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_is_property_subject_to_lease_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_is_property_subject_to_lease_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_net_operating_income2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_net_operating_income2_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_net_operating_income_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_net_operating_income_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_occupancy_rate_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_occupancy_rate_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_pet_deposit2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_pet_deposit2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_pet_deposit_refundable2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_pet_deposit_refundable2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_pet_fee_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_pet_fee_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_pets5_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_pets5_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_property_manager_name2_info,'None',''),', ',','),',,',',')),',')),''),',') as 	ncrmls_rental_information_property_manager_name2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_property_manager_name_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_property_manager_name_info

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_references_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_references_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_renters_insurance_required_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_renters_insurance_required_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_security_deposit2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_security_deposit2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_smoking2_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_smoking2_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(ncrmls_rental_information_total_actual_rent_info,'None',''),', ',','),',,',',')),',')),''),',') as ncrmls_rental_information_total_actual_rent_info



,is_rental_restrictions_1_month
,is_rental_restrictions_1_week
,is_rental_restrictions_1_year
,is_rental_restrictions_2_months
,is_rental_restrictions_2_weeks
,is_rental_restrictions_3_days
,is_rental_restrictions_3_months
,is_rental_restrictions_4_months
,is_rental_restrictions_5_months
,is_rental_restrictions_6_months
,is_rental_restrictions_7_months
,is_rental_restrictions_no_lease_1st_year
,is_rental_restrictions_no_minimum
,is_rental_restrictions_no_rentals_allowed
,is_rental_restrictions_other
,is_rental_restrictions_tenant_approval
,entry_level
,recolorado_structural_style_new_home_com
,recolorado_structural_style_new_home_plan
,recolorado_structural_style_new_home_spec
,mtrmls_waterfront_summer_access
,mtrmls_waterfront_year_round_access
,naar_lake_chain_info 
,naar_lake_depth_info 
,naar_zoning_lot 
,naar_lake_bottom_info 
,naar_water_frontage_ft
,wcbor_is_bring_own_builder
,wcbor_is_approved_builder
,wcbor_is_designated_builder
,ctarmls_rooms_ceiling_cathedral_vaulted
,ctarmls_rooms_ceiling_fan_s
,ctarmls_rooms_ceiling_smooth
,ctarmls_rooms_eat_in_kitchen
,ctarmls_rooms_elevator
,ctarmls_rooms_entrance_foyer
,ctarmls_rooms_family
,ctarmls_rooms_formal_living
,ctarmls_rooms_frog_attached
,ctarmls_rooms_frog_detached
,ctarmls_rooms_game
,ctarmls_rooms_garden_tub_shower
,ctarmls_rooms_great
,ctarmls_rooms_high_ceilings
,ctarmls_rooms_in_law_floorplan
,ctarmls_rooms_kitchen_island
,ctarmls_rooms_living_dining_combo
,ctarmls_rooms_loft
,ctarmls_rooms_media
,ctarmls_rooms_office
,ctarmls_rooms_other_use_remarks
,ctarmls_rooms_pantry
,ctarmls_rooms_separate_dining
,ctarmls_rooms_sauna
,ctarmls_rooms_study
,ctarmls_rooms_sun
,ctarmls_rooms_tray_ceiling_s
,ctarmls_rooms_utility
,ctarmls_rooms_walk_in_closet_s
,ctarmls_rooms_wet_bar
,ctarmls_rooms_wine_cellar
,ctarmls_rooms_beamed_ceilings
,ctarmls_rooms_bonus
,ctarmls_rooms_ceiling_blown


from stage.direct_idx_attribute_custom_3 s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
where s.source_id in {}  and t.target_listing_id is not NULL;
"""

LISTING_ATTRIBUTE_CUSTOM_QUERY_4 = """
select 
t.source_id as source_id     
,t.batch_id as batch_id      
,t.target_listing_id   as listing_id  
,t.y_creation_date      
,y_last_update_date


,neren_architectural_style_a_frame 
,neren_architectural_style_adirondack 
,neren_architectural_style_antique 
,neren_architectural_style_apartment_building 
,neren_architectural_style_arts_and_crafts 
,neren_architectural_style_barndominium 
,neren_architectural_style_bungalow 
,neren_architectural_style_cabin 
,neren_architectural_style_cape 
,neren_architectural_style_carriage 
,neren_architectural_style_chalet 
,neren_architectural_style_colonial 
,neren_architectural_style_condex 
,neren_architectural_style_contemporary 
,neren_architectural_style_conversion 
,neren_architectural_style_cottage_camp 
,neren_architectural_style_craftsman 
,neren_architectural_style_deck_house 
,neren_architectural_style_detached 
,neren_architectural_style_double_wide 
,neren_architectural_style_duplex 
,neren_architectural_style_end_row 
,neren_architectural_style_end_unit 
,neren_architectural_style_farmhouse 
,neren_architectural_style_federal 
,neren_architectural_style_flat 
,neren_architectural_style_four_square 
,neren_architectural_style_freestanding 
,neren_architectural_style_gambrel 
,neren_architectural_style_garden 
,neren_architectural_style_garrison 
,neren_architectural_style_georgian 
,neren_architectural_style_greek_revival 
,neren_architectural_style_ground_floor 
,neren_architectural_style_high_rise 
,neren_architectural_style_historic_vintage 
,neren_architectural_style_hotel 
,neren_architectural_style_inside_row 
,neren_architectural_style_log 
,neren_architectural_style_manuf_mobile 
,neren_architectural_style_modern_architecture 
,neren_architectural_style_modified 
,neren_architectural_style_multi_family 
,neren_architectural_style_multi_level 
,neren_architectural_style_new_englander 
,neren_architectural_style_octagon 
,neren_architectural_style_other 
,neren_architectural_style_post_and_beam 
,neren_architectural_style_raised_ranch 
,neren_architectural_style_ranch 
,neren_architectural_style_rehab_needed 
,neren_architectural_style_reproduction 
,neren_architectural_style_rooming_house 
,neren_architectural_style_saltbox 
,neren_architectural_style_single_level 
,neren_architectural_style_single_wide 
,neren_architectural_style_split_entry 
,neren_architectural_style_split_level 
,neren_architectural_style_straw_bale 
,neren_architectural_style_studio 
,neren_architectural_style_tiny_home 
,neren_architectural_style_top_floor 
,neren_architectural_style_townhouse 
,neren_architectural_style_tri_level 
,neren_architectural_style_tudor 
,neren_architectural_style_victorian 
,neren_architectural_style_w_addition 
,neren_architectural_style_walkout_lower_level 
,neren_architectural_style_yurt 
,neren_lot_features_agricultural 
,neren_lot_features_airport_community 
,neren_lot_features_alternative_lots_avail 
,neren_lot_features_alternative_styles_avail 
,neren_lot_features_beach_access 
,neren_lot_features_canal 
,neren_lot_features_city_lot 
,neren_lot_features_condo_development 
,neren_lot_features_conserved_land 
,neren_lot_features_corner 
,neren_lot_features_country_setting 
,neren_lot_features_curbing 
,neren_lot_features_deed_restricted 
,neren_lot_features_deep_water_access 
,neren_lot_features_farm 
,neren_lot_features_farm_dairy 
,neren_lot_features_farm_horse_animal 
,neren_lot_features_field_pasture 
,neren_lot_features_hilly 
,neren_lot_features_interior_lot 
,neren_lot_features_lake_access 
,neren_lot_features_lake_frontage 
,neren_lot_features_lake_view 
,neren_lot_features_lakes 
,neren_lot_features_landscaped 
,neren_lot_features_leased 
,neren_lot_features_level 
,neren_lot_features_lowland 
,neren_lot_features_major_road_frontage 
,neren_lot_features_mountain_view 
,neren_lot_features_near_schools 
,neren_lot_features_neighbor_business 
,neren_lot_features_open 
,neren_lot_features_orchards 
,neren_lot_features_other 
,neren_lot_features_pond 
,neren_lot_features_pond_frontage 
,neren_lot_features_pond_site 
,neren_lot_features_prd_pud 
,neren_lot_features_recreational 
,neren_lot_features_rental_complex 
,neren_lot_features_river 
,neren_lot_features_river_frontage 
,neren_lot_features_rolling 
,neren_lot_features_secluded 
,neren_lot_features_sidewalks 
,neren_lot_features_ski_area 
,neren_lot_features_ski_trailside 
,neren_lot_features_slight 
,neren_lot_features_sloping 
,neren_lot_features_special_assessment 
,neren_lot_features_steep 
,neren_lot_features_stream 
,neren_lot_features_street_lights 
,neren_lot_features_subdivision 
,neren_lot_features_timber 
,neren_lot_features_trail_near_trail 
,neren_lot_features_view 
,neren_lot_features_walking_trails 
,neren_lot_features_water_view 
,neren_lot_features_waterfall 
,neren_lot_features_waterfront 
,neren_lot_features_wetlands 
,neren_lot_features_wooded 
,neren_features_interior_attic_hatch_skuttle 
,neren_features_interior_attic_no_access 
,neren_features_interior_attic_pulldown 
,neren_features_interior_attic_walkup 
,neren_features_interior_bar 
,neren_features_interior_blinds 
,neren_features_interior_cathedral_ceiling 
,neren_features_interior_cedar_closet 
,neren_features_interior_ceiling_fan 
,neren_features_interior_central_vacuum 
,neren_features_interior_coin_laundry 
,neren_features_interior_common_heating_cooling 
,neren_features_interior_dining_area 
,neren_features_interior_draperies 
,neren_features_interior_elevator 
,neren_features_interior_elevator_freight 
,neren_features_interior_elevator_passenger 
,neren_features_interior_fireplace_gas 
,neren_features_interior_fireplace_screens_equip 
,neren_features_interior_fireplace_wood 
,neren_features_interior_fireplaces_1 
,neren_features_interior_fireplaces_2 
,neren_features_interior_fireplaces_3_plus 
,neren_features_interior_furnished 
,neren_features_interior_hearth 
,neren_features_interior_home_theatre_wiring 
,neren_features_interior_hot_tub 
,neren_features_interior_in_law_suite 
,neren_features_interior_in_law_accessory_dwelling 
,neren_features_interior_kitchen_island 
,neren_features_interior_kitchen_dining 
,neren_features_interior_kitchen_family 
,neren_features_interior_kitchen_living 
,neren_features_interior_laundry_1st_floor 
,neren_features_interior_laundry_2nd_floor 
,neren_features_interior_laundry_basement 
,neren_features_interior_laundry_hook_ups 
,neren_features_interior_lead_stain_glass 
,neren_features_interior_light_fixtures__enrgy_rtd 
,neren_features_interior_lighting_led 
,neren_features_interior_lighting_t8_fluorescent 
,neren_features_interior_lighting_contrls__respnsv 
,neren_features_interior_living_dining 
,neren_features_interior_natural_light 
,neren_features_interior_natural_woodwork 
,neren_features_interior_other 
,neren_features_interior_pool_indoor 
,neren_features_interior_pot_filler 
,neren_features_interior_primary_br_w__ba 
,neren_features_interior_programmable_thermostat 
,neren_features_interior_sauna 
,neren_features_interior_security 
,neren_features_interior_security_doors 
,neren_features_interior_skylight 
,neren_features_interior_skylights_energy_rated 
,neren_features_interior_smart_thermostat 
,neren_features_interior_soaking_tub 
,neren_features_interior_solar_tubes 
,neren_features_interior_storage_indoor 
,neren_features_interior_surround_sound_wiring 
,neren_features_interior_vaulted_ceiling 
,neren_features_interior_walk_in_closet 
,neren_features_interior_walk_in_pantry 
,neren_features_interior_wet_bar 
,neren_features_interior_whirlpool_tub 
,neren_features_interior_window_treatment 
,neren_features_interior_wood_stove_hook_up 
,neren_features_interior_wood_stove_insert 
,neren_cooling_attic_fan 
,neren_cooling_central_ac 
,neren_cooling_mini_split 
,neren_cooling_multi_zone 
,neren_cooling_none 
,neren_cooling_other 
,neren_cooling_wall_ac_units 
,neren_cooling_whole_house_fan 
,neren_basement_description_apartments 
,neren_basement_description_bulkhead 
,neren_basement_description_climate_controlled 
,neren_basement_description_concrete 
,neren_basement_description_concrete_floor 
,neren_basement_description_crawl_space 
,neren_basement_description_daylight 
,neren_basement_description_dirt 
,neren_basement_description_dirt_floor 
,neren_basement_description_exterior_access 
,neren_basement_description_finished 
,neren_basement_description_frost_wall 
,neren_basement_description_full 
,neren_basement_description_gravel 
,neren_basement_description_insulated 
,neren_basement_description_interior_access 
,neren_basement_description_no_tenant_access 
,neren_basement_description_none 
,neren_basement_description_other 
,neren_basement_description_partial 
,neren_basement_description_partially_finished 
,neren_basement_description_roughed_in 
,neren_basement_description_slab 
,neren_basement_description_stairs_basement 
,neren_basement_description_stairs_exterior 
,neren_basement_description_stairs_interior 
,neren_basement_description_storage_assigned 
,neren_basement_description_storage_locked 
,neren_basement_description_storage_space 
,neren_basement_description_stubbed_in 
,neren_basement_description_sump_pump 
,neren_basement_description_unfinished 
,neren_basement_description_walkout 
,neren_features_accessibility_1st_floor_1_2_bathroom 
,neren_features_accessibility_1st_floor_3_ft_doors 
,neren_features_accessibility_1st_floor_3_4_bathroom 
,neren_features_accessibility_1st_floor_bedroom 
,neren_features_accessibility_1st_floor_full_bathroom 
,neren_features_accessibility_1st_floor_hrd_surfce_flr 
,neren_features_accessibility_1st_floor_laundry 
,neren_features_accessibility_1st_floor_low_pile_carpet 
,neren_features_accessibility_3_ft_doors 
,neren_features_accessibility_access_common_use_areas 
,neren_features_accessibility_access_laundry_no_steps 
,neren_features_accessibility_access_mailboxes_no_steps 
,neren_features_accessibility_access_parking 
,neren_features_accessibility_access_restrooms 
,neren_features_accessibility_accessibility_features 
,neren_features_accessibility_bathroom_blocking_in_wall 
,neren_features_accessibility_bathroom_w_5_ft_diameter 
,neren_features_accessibility_bathroom_w_roll_in_shower 
,neren_features_accessibility_bathroom_w_step_in_shower 
,neren_features_accessibility_bathroom_w_tub 
,neren_features_accessibility_bathroom_w_wall_blocking 
,neren_features_accessibility_easy_grip_door_hardware 
,neren_features_accessibility_grab_bars_in_bathroom 
,neren_features_accessibility_handicap_modified 
,neren_features_accessibility_hard_surface_flooring 
,neren_features_accessibility_kitchen_w_5_ft_diameter 
,neren_features_accessibility_kitchenette_w_5_ft_diam 
,neren_features_accessibility_low_pile_carpet 
,neren_features_accessibility_low_pressure_door_opening 
,neren_features_accessibility_multi_level_bus_w_elevatr 
,neren_features_accessibility_multi_level_w_4_ft_stairs 
,neren_features_accessibility_multi_level_w_lift 
,neren_features_accessibility_multi_level_w_stack_clost 
,neren_features_accessibility_no_stairs 
,neren_features_accessibility_no_stairs_from_parking 
,neren_features_accessibility_one_level_business 
,neren_features_accessibility_one_level_home 
,neren_features_accessibility_paved_parking 
,neren_features_accessibility_zero_step_entry_ramp 
,neren_sewer_1000_gallon 
,neren_sewer_1250_gallon 
,neren_sewer_1500_plus_gallon 
,neren_sewer_500_gallon 
,neren_sewer_750_gallon 
,neren_sewer_alternative_system 
,neren_sewer_cesspool 
,neren_sewer_community 
,neren_sewer_concrete 
,neren_sewer_deeded 
,neren_sewer_drywell 
,neren_sewer_grey_water 
,neren_sewer_holding_tank 
,neren_sewer_leach_field 
,neren_sewer_leach_field_at_grade 
,neren_sewer_leach_field_conventionl 
,neren_sewer_leach_field_existing 
,neren_sewer_leach_field_mound 
,neren_sewer_leach_field_off_site 
,neren_sewer_leach_field_on_site 
,neren_sewer_metal 
,neren_sewer_metered 
,neren_sewer_mound 
,neren_sewer_none 
,neren_sewer_on_site_septic_exists 
,neren_sewer_on_site_septic_needed 
,neren_sewer_other 
,neren_sewer_plastic 
,neren_sewer_private 
,neren_sewer_private_available 
,neren_sewer_public 
,neren_sewer_public_available 
,neren_sewer_public_sewer_at_street 
,neren_sewer_public_sewer_on_site 
,neren_sewer_pump_up 
,neren_sewer_pumping_station 
,neren_sewer_replacement_field_offsite 
,neren_sewer_replacement_field_onsite 
,neren_sewer_replacement_leach_field 
,neren_sewer_septic 
,neren_sewer_septic_design_available 
,neren_sewer_septic_shared 
,neren_sewer_shared 
,neren_sewer_soil_test_available 
,neren_sewer_unknown 
,neren_utilities_cable 
,neren_utilities_cable_at_site 
,neren_utilities_cable_available 
,neren_utilities_gas_at_street 
,neren_utilities_gas_lp_bottle 
,neren_utilities_gas_on_site 
,neren_utilities_gas_underground 
,neren_utilities_multi_phone_lines 
,neren_utilities_none 
,neren_utilities_oil_tank_underground 
,neren_utilities_other 
,neren_utilities_phone 
,neren_utilities_satellite 
,neren_utilities_t1_available 
,neren_utilities_telephone_at_site 
,neren_utilities_telephone_available 
,neren_utilities_underground_utilities 
,neren_water_source_cistern 
,neren_water_source_community 
,neren_water_source_deeded 
,neren_water_source_drilled_well 
,neren_water_source_driven_point 
,neren_water_source_dug_well 
,neren_water_source_energy_star 
,neren_water_source_flat_rate 
,neren_water_source_grey_water_reuse 
,neren_water_source_included 
,neren_water_source_infrared_light 
,neren_water_source_lake_pond 
,neren_water_source_metered 
,neren_water_source_none 
,neren_water_source_on_site_well_exists 
,neren_water_source_on_site_well_needed 
,neren_water_source_other 
,neren_water_source_private 
,neren_water_source_public 
,neren_water_source_public_water_at_street 
,neren_water_source_public_water_on_site 
,neren_water_source_purifier_soft 
,neren_water_source_reclaimed 
,neren_water_source_reverse_osmosis 
,neren_water_source_shared 
,neren_water_source_spring 
,neren_water_source_ultraviolet 
,neren_water_source_unknown 
,neren_features_exterior_balcony 
,neren_features_exterior_barn 
,neren_features_exterior_basketball_court 
,neren_features_exterior_beach_access 
,neren_features_exterior_berth 
,neren_features_exterior_boat_house 
,neren_features_exterior_boat_launch 
,neren_features_exterior_boat_mooring 
,neren_features_exterior_boat_slip_dock 
,neren_features_exterior_building 
,neren_features_exterior_built_in_gas_grill 
,neren_features_exterior_covered_slip 
,neren_features_exterior_day_berth 
,neren_features_exterior_day_dock 
,neren_features_exterior_deck 
,neren_features_exterior_docks 
,neren_features_exterior_doors_energy_star 
,neren_features_exterior_dry_berth 
,neren_features_exterior_dry_dock 
,neren_features_exterior_fence_dog 
,neren_features_exterior_fence_full 
,neren_features_exterior_fence_invisible_pet 
,neren_features_exterior_fence_partial 
,neren_features_exterior_garden_space 
,neren_features_exterior_gazebo 
,neren_features_exterior_greenhouse 
,neren_features_exterior_guest_house 
,neren_features_exterior_handicap_modified 
,neren_features_exterior_hot_tub 
,neren_features_exterior_natural_shade 
,neren_features_exterior_other 
,neren_features_exterior_other_see_remarks 
,neren_features_exterior_outbuilding 
,neren_features_exterior_patio 
,neren_features_exterior_playground 
,neren_features_exterior_pool_above_ground 
,neren_features_exterior_pool_in_ground 
,neren_features_exterior_porch 
,neren_features_exterior_porch_covered 
,neren_features_exterior_porch_enclosed 
,neren_features_exterior_porch_heated 
,neren_features_exterior_porch_screened 
,neren_features_exterior_poultry_coop 
,neren_features_exterior_private_dock 
,neren_features_exterior_rack 
,neren_features_exterior_row_to_water 
,neren_features_exterior_sauna 
,neren_features_exterior_shed 
,neren_features_exterior_slip 
,neren_features_exterior_stables 
,neren_features_exterior_storage 
,neren_features_exterior_tennis_court 
,neren_features_exterior_trash 
,neren_features_exterior_window_screens 
,neren_features_exterior_windows_double_pane 
,neren_features_exterior_windows_energy_star 
,neren_features_exterior_windows_high_impact 
,neren_features_exterior_windows_low_e 
,neren_features_exterior_windows_solar_shades 
,neren_features_exterior_windows_storm 
,neren_features_exterior_windows_tinted 
,neren_features_exterior_windows_triple_pane 
,neren_heat_fuel_coal 
,neren_heat_fuel_convection 
,neren_heat_fuel_electric 
,neren_heat_fuel_gas_lp_bottle 
,neren_heat_fuel_gas_natural 
,neren_heat_fuel_gas_natural_available 
,neren_heat_fuel_geothermal 
,neren_heat_fuel_gravity 
,neren_heat_fuel_kerosene 
,neren_heat_fuel_multi_fuel 
,neren_heat_fuel_none 
,neren_heat_fuel_oil 
,neren_heat_fuel_other 
,neren_heat_fuel_pellet 
,neren_heat_fuel_solar 
,neren_heat_fuel_wood 
,omdreb_parking_none 
,omdreb_parking_outside_surface_open 
,omdreb_parking_covered_parking 
,omdreb_parking_front_yard_parking 
,omdreb_parking_street_parking_only 
,omdreb_parking_private_drive_triple_wide 
,omdreb_parking_private_drive_double_wide 
,omdreb_parking_private_drive_single_wide 
,omdreb_parking_visitor_parking 
,omdreb_parking_boulevard_parking 
,omdreb_parking_carport_parking 
,omdreb_parking_rv_truck_parking 
,omdreb_parking_lane_alley_parking 
, maris_sewer_aerobic_septic
, maris_sewer_community_sewer
, maris_sewer_lagoon
, maris_sewer_lift_system
, maris_sewer_none
, maris_sewer_not_connected
, maris_sewer_other
, maris_sewer_private_sewer
, maris_sewer_public_sewer
, maris_sewer_septic_tank
, maris_sewer_sewer_main_10_inch
, maris_sewer_sewer_main_12_inch
, maris_sewer_sewer_main_4_inch
, maris_sewer_sewer_main_6_inch
, maris_sewer_sewer_main_8_inch
, maris_sewer_sewer_main_over_12_inch
, maris_sewer_shared_septic
, maris_sewer_terre_du_lac
, maris_utilities_cable_available
, maris_utilities_electricity_available
, maris_utilities_gas_to_site
, maris_utilities_none
, maris_utilities_other
, maris_utilities_phone_available
, maris_utilities_sewer_available
, maris_utilities_water_available
, maris_water_cistern
, maris_water_community
, maris_water_in_not_connected
, maris_water_lake_water
, maris_water_lo_flow_fixtures
, maris_water_none
, maris_water_other
, maris_water_pond
, maris_water_public
, maris_water_river
, maris_water_shared
, maris_water_spring
, maris_water_stream_water
, maris_water_water_main_10_inch
, maris_water_water_main_12_inch
, maris_water_water_main_1_inch
, maris_water_water_main_2_inch
, maris_water_water_main_4_inch
, maris_water_water_main_6_inch
, maris_water_water_main_8_inch
, maris_water_water_main_over_10_inch
, maris_water_water_main_over_12_inch
, maris_water_well

,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_accessibility_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_accessibility_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_appliances_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_appliances_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_boating_amenities_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_boating_amenities_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_ceiling_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_ceiling_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_condo_amenities_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_condo_amenities_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_cooling_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_cooling_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_design_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_design_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_doors_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_doors_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_ext_fin_trim_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_ext_fin_trim_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_fees_include_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_fees_include_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_financing_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_financing_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_flooring_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_flooring_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_furnishing_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_furnishing_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_govern_bodies_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_govern_bodies_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_heating_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_heating_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_master_bath_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_master_bath_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_master_bedroom_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_master_bedroom_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_misc_exterior_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_misc_exterior_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_occupant_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_occupant_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_parking_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_parking_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_possession_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_possession_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_restrictions_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_restrictions_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_rooms_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_rooms_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_secondary_location_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_secondary_location_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_security_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_security_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_sewer_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_sewer_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_showing_instructions_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_showing_instructions_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_special_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_special_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_view_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_view_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_water_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_water_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_waterfront_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_waterfront_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_windows_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_windows_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_association_fee_includes_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_association_fee_includes_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_other_structures_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_other_structures_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_utilities_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_utilities_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_primary_location_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_primary_location_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_coastal_construction_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_coastal_construction_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_island_location_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_island_location_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_property_condition_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_property_condition_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_sqft_source_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_sqft_source_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_master_fee_period_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_master_fee_period_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_rent_type_min_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_rent_type_min_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_living_sqft_source_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_living_sqft_source_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_association_fee_period_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_association_fee_period_info
,sancap_level_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_condo_name_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_condo_name_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_other_fees_period_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_other_fees_period_info
,string_to_array(nullif(TRIM(TRIM(TRIM(REPLACE(REPLACE(REPLACE(Replace(Replace(sancap_pets_info,'"',''),'''',''),'None',''),', ',','),',,',',')),',')),''),',') as sancap_pets_info
,his_exterior_finish_above_ground
,his_exterior_finish_brick_4_in_ht
,his_exterior_finish_brick_8_in_ht
,his_exterior_finish_board_and_batten
,his_exterior_finish_bamboo
,his_exterior_finish_brick
,his_exterior_finish_concrete_block
,his_exterior_finish_concrete
,his_exterior_finish_fiber_cement_siding
,his_exterior_finish_fir_pine
,his_exterior_finish_glass
,his_exterior_finish_masonite
,his_exterior_finish_plaster
,his_exterior_finish_concrete_reinforced
,his_exterior_finish_redwood_cedar
,his_exterior_finish_slab
,his_exterior_finish_steel
,his_exterior_finish_shake
,his_exterior_finish_shiplap_siding
,his_exterior_finish_stone
,his_exterior_finish_other
,his_exterior_finish_masonry_stucco
,his_exterior_finish_vinyl
,his_exterior_finish_wood
,his_heating_none
,his_heating_heat_pump
,his_heating_solar
,his_heating_zoned
,his_heating_other
,his_heating_central_air_filtration
,his_heating_partial
,his_cooling_none
,his_cooling_heat_pump
,his_cooling_zoned
,his_cooling_wall_window_unit
,his_cooling_other
,his_cooling_air_conditioning
,his_cooling_central_air_filtration
,his_cooling_partial
,his_cooling_varies_by_unit
,his_cooling_central_air
,his_pool_gunite
,his_pool_fiberglass
,his_pool_heated
,his_pool_in_ground
,his_pool_above_ground
,his_pool_indoor
,his_pool_outdoor_pool
,his_pool_liner
,his_pool_vinyl
,his_pool_pool_spa_combo
,his_pool_tile
,his_pool_other
,his_pool_plaster
, his_topography_fairly_level
, his_topography_gentle_slope
, his_topography_graded
, his_topography_hilly
, his_topography_level
, his_topography_rolling_terrain
, his_topography_steep_slope
, his_topography_terraced
, his_topography_other
,glvmls_guest_house_bathroom
,glvmls_guest_house_bedroom
,glvmls_guest_house_entry_to_main_house
,glvmls_guest_house_kitchenette
,glvmls_guest_house_separate_entry
,crea_community_adult_oriented
,crea_community_family_oriented
,crea_community_golf_course_development
,crea_community_mobiles_allowed
,crea_community_pets_not_allowed
,crea_community_quiet_area
,crea_community_rural_setting
,crea_community_seniors_oriented
,crea_community_high_traffic_area
,crea_community_lake_privileges
,crea_community_school_bus
,crea_community_public_swimming_pool
,crea_community_pets_allowed
,crea_community_bus_route
,crea_community_community_centre
,crea_community_fishing
,crea_community_industrial_park
,crea_community_public_washrooms
,crea_community_recreational_facilities
,crea_community_high_population_density
,crea_community_pet_restrictions
,crea_community_pets_allowed_with_restrictions
,crea_community_rentals_allowed
,crea_community_rentals_not_allowed
,crea_community_rentals_allowed_with_restrictions
,crea_community_age_restrictions
,crea_security_securityfeatures
,crea_security_alarm_system
,crea_security_security_system
,crea_security_security_guard
,crea_security_security
,crea_security_split_security_man_electric
,crea_security_smoke_detectors
,crea_security_sprinkler_system_fire
,crea_security_controlled_entry
,crea_security_fire_alarm_system
,crea_security_monitored_alarm
,crea_security_security_window_bars
,crea_security_full_sprinkler_system
,crea_security_no_sprinkler_system
,crea_security_none
,crea_security_partial_sprinkler_system
,crea_security_smoke_detector_only
,crea_heating_heat_pump
,crea_heating_air_circulation_heat
,crea_heating_hot_water_radiator_heat
,crea_heating_baseboard_heaters
,crea_heating_electric_baseboard_units
,crea_heating_space_heating_baseboards
,crea_heating_radiant_heat
,crea_heating_steam_radiator
,crea_heating_stove
,crea_heating_slow_burning_stove
,crea_heating_forced_air
,crea_heating_radiant_infra_red_heat
,crea_heating_underfloor_ducts
,crea_heating_wall_heaters
,crea_heating_gravity_heat_system
,crea_heating_overhead_heaters
,crea_heating_ground_source_heat
,crea_heating_not_known
,crea_heating_no_heat
,crea_heating_in_floor_heating
,crea_heating_outside_furnace
,crea_heating_boiler
,crea_heating_space_heater
,crea_heating_heat_recovery_ventilation_hrv
,crea_heating_high_efficiency_furnace
,crea_heating_see_remarks
,crea_heating_central_heating
,crea_heating_floor_heat
,crea_heating_coil_fan
,crea_heating_other
,crea_heating_radiator
,crea_heating_electric_air_cleaner
,crea_heating_hot_water
,crea_heating_wood_stove
,crea_heating_wall_mounted_heat_pump
,crea_heating_central_heat_pump
,crea_heating_ductless
,crea_heating_furnace
,crea_heating_mini_split
,crea_heating_oil
,crea_heating_heating_oil
,crea_heating_stove_oil
,crea_heating_waste_oil
,crea_heating_electric
,crea_heating_natural_gas
,crea_heating_propane
,crea_heating_combination
,crea_heating_solar
,crea_heating_wood
,crea_heating_pellet
,crea_heating_coal
,crea_heating_bi_energy
,crea_heating_unknown
,crea_heating_geo_thermal
,crea_heating_wind_power
,wfrmls_hoa_amenities_alarm_system_paid
,wfrmls_hoa_amenities_barbecue
,wfrmls_hoa_amenities_biking_trails
,wfrmls_hoa_amenities_bocce_ball_court
,wfrmls_hoa_amenities_cable_tv
,wfrmls_hoa_amenities_clubhouse
,wfrmls_hoa_amenities_concierge
,wfrmls_hoa_amenities_controlled_access
,wfrmls_hoa_amenities_earthquake_insurance
,wfrmls_hoa_amenities_electricity
,wfrmls_hoa_amenities_fire_pit
,wfrmls_hoa_amenities_fitness_center
,wfrmls_hoa_amenities_gas
,wfrmls_hoa_amenities_gated
,wfrmls_hoa_amenities_golf_course
,wfrmls_hoa_amenities_hiking_trails
,wfrmls_hoa_amenities_horse_trails
,wfrmls_hoa_amenities_insurance
,wfrmls_hoa_amenities_maintenance
,wfrmls_hoa_amenities_management
,wfrmls_hoa_amenities_on_site_security
,wfrmls_hoa_amenities_other
,wfrmls_hoa_amenities_pet_rules
,wfrmls_hoa_amenities_pets_not_permitted
,wfrmls_hoa_amenities_pets_permitted
,wfrmls_hoa_amenities_picnic_area
,wfrmls_hoa_amenities_playground
,wfrmls_hoa_amenities_pool
,wfrmls_hoa_amenities_racquetball
,wfrmls_hoa_amenities_rv_parking
,wfrmls_hoa_amenities_sauna
,wfrmls_hoa_amenities_security
,wfrmls_hoa_amenities_sewer_paid
,wfrmls_hoa_amenities_snow_removal
,wfrmls_hoa_amenities_spa_hot_tub
,wfrmls_hoa_amenities_storage
,wfrmls_hoa_amenities_tennis_courts
,wfrmls_hoa_amenities_trash
,wfrmls_hoa_amenities_water
,ccmls_community_owner_golf_cart
,ccmls_community_owner_motorcycle
,sancap_island_location_east_end
,sancap_island_location_mid_island
,sancap_island_location_west_end
,sancap_location_bayfront
,sancap_location_bayou
,sancap_location_canal
,sancap_location_golf
,sancap_location_gulf
,sancap_location_inland
,sancap_location_lake
,sancap_location_near_beach
,sancap_location_river
,sancap_location_roosevelt
,treb_access_atv_4_wd_only
,treb_access_by_water
,treb_access_fees_apply
,treb_access_highway
,treb_access_marina_docking
,treb_access_municipal_road
,treb_access_no_road
,treb_access_other
,treb_access_paved_road
,treb_access_private_docking
,treb_access_private_road
,treb_access_public_docking
,treb_access_public_road
,treb_access_r_o_w_deeded
,treb_access_r_o_w_not_deeded
,treb_access_seasonal_municipal_road
,treb_access_seasonal_private_road
,treb_access_unknown
,treb_access_water_only
,treb_access_year_round_municipal_road
,treb_access_year_round_private_road
,tarmls_green_nahb_bronze
,tarmls_green_nahb_emerald
,tarmls_green_nahb_gold
,tarmls_green_reg_res_grn_bronze
,tarmls_green_reg_res_grn_emerald
,tarmls_green_reg_res_grn_gold
,tarmls_green_reg_res_grn_silver
,tarmls_green_nahb_silver
,tarmls_green_usgbc_leed_cert
,tarmls_green_usgbc_leed_gold
,tarmls_green_usgbc_leed_platnm
,tarmls_green_usgbc_leed_silver
,tarmls_green_enrgy_star_light_pkg
,tarmls_green_enrgy_smart_home_rtg
,tarmls_green_enrgy_star_appliance
,tarmls_green_solar_hot_water_sys
,tarmls_green_enrgy_star_qualified
,tarmls_green_govt_ee_prog_crt
,tarmls_green_hers_rating
,tarmls_green_utlty_co_ee_prog_crt
,tarmls_green_air_quality
,tarmls_green_building_materials
,tarmls_green_electric
,tarmls_green_heating_or_cooling
,tarmls_green_solar
,tarmls_green_water
,tarmls_green_bath_exhaust_out
,tarmls_green_green_seal_paints
,tarmls_green_adobe
,tarmls_green_ins_concrete_forms
,tarmls_green_rammed_earth
,tarmls_green_strawbale
,tarmls_green_dual_flush_toilets
,tarmls_green_low_flow_faucets
,tarmls_green_low_flow_showerheads
,tarmls_green_rainwater_harvesting
,tarmls_green_water_sense_shower_head
,tarmls_green_water_sense_faucets
,treb_balcony_enclosed
,treb_balcony_juliette
,treb_balcony_none
,treb_balcony_open
,treb_balcony_terrace
,treb_balcony_unknown
,treb_heating_baseboard
,treb_heating_electric_forced_air
,treb_heating_electric_hot_water
,treb_heating_fan_coil
,treb_heating_forced_air
,treb_heating_gas_forced_air_closed
,treb_heating_gas_forced_air_open
,treb_heating_gas_hot_water
,treb_heating_heat_pump
,treb_heating_none
,treb_heating_oil_forced_air
,treb_heating_oil_hot_water
,treb_heating_oil_steam
,treb_heating_other
,treb_heating_propane_gas
,treb_heating_radiant
,treb_heating_solar
,treb_heating_steam_radiators
,treb_heating_water
,treb_heating_water_radiators
,treb_heating_woodburning
,treb_heating_unknown
,treb_cooling_central_air
,treb_cooling_none
,treb_cooling_other
,treb_cooling_partial
,treb_cooling_wall_unit
,treb_cooling_window_unit
,treb_cooling_unknown
,treb_laundry_coin_operated
,treb_laundry_ensuite
,treb_laundry_in_area
,treb_laundry_none
,treb_laundry_set_usage
,treb_laundry_shared
,treb_laundry_common_area
,treb_laundry_electric_dryer_hookup
,treb_laundry_gas_dryer_hookup
,treb_laundry_in_basement
,treb_laundry_in_bathroom
,treb_laundry_in_building
,treb_laundry_in_carport
,treb_laundry_in_garage
,treb_laundry_in_hall
,treb_laundry_in_kitchen
,treb_laundry_in_suite_laundry
,treb_laundry_inside
,treb_laundry_laundry_chute
,treb_laundry_laundry_closet
,treb_laundry_laundry_room
,treb_laundry_multiple_locations
,treb_laundry_other
,treb_laundry_outside
,treb_laundry_sink
,treb_laundry_washer_hookup
,treb4_occupant_owner_tenant
,treb4_occupant_owner
,treb4_occupant_partial
,treb4_occupant_tenant
,treb4_occupant_vacant
,tarmls_extra_room_arizona
,tarmls_extra_room_bonus
,tarmls_extra_room_dark
,tarmls_extra_room_den
,tarmls_extra_room_excercise
,tarmls_extra_room_excercise2
,tarmls_extra_room_library
,tarmls_extra_room_library2
,tarmls_extra_room_loft
,tarmls_extra_room_media
,tarmls_extra_room_media2
,tarmls_extra_room_office
,tarmls_extra_room_office3
,tarmls_extra_room_rec
,tarmls_extra_room_storage
,tarmls_extra_room_studio
,tarmls_extra_room_workshop
,tarmls_extra_room_none
,tarmls_extra_room_den2
,tarmls_extra_room_library3
,tarmls_extra_room_other
,mfrmls_horse_barn
,mfrmls_horse_shed
,mfrmls_horse_storage
,mfrmls_horse_num_of_paddocks_pastures
,mfrmls_horse_num_of_stalls
,mfrmls_horse_riding_ring
,mfrmls_horse_stables
,mfrmls_horse_arena
,mfrmls_horse_horse_barn
,mfrmls_horse_pole_barn
,mfrmls_horse_tack_room
,mfrmls_horse_washrack
,mfrmls_lease_times_per_year
,mfrmls_years_prior_lease
,mfrmls_owned_prior_lease
,ctmls_lot_approved_building_lot
,ctmls_lot_approved_subdivision
,ctmls_lot_barn
,ctmls_lot_curb_cut
,ctmls_lot_curbs_gutters
,ctmls_lot_finish_graded
,ctmls_lot_garage
,ctmls_lot_horse_stable
,ctmls_lot_house
,ctmls_lot_lot_staked
,ctmls_lot_none
,ctmls_lot_rough_graded
,ctmls_lot_shed
,ctmls_lot_sidewalk
,ctmls_lot_storm_drain
,mfrmls_number_of_pets
,mfrmls_has_years_prior_lease
,mfrmls_has_no_years_prior_lease
,miamire_lease_per_year
,creb_laundry_in_basement
,creb_laundry_in_bathroom
,creb_laundry_coin_operated
,creb_laundry_common_area
,creb_laundry_electric_dryer_hookup
,creb_laundry_in_garage
,creb_laundry_gas_dryer_hookup
,creb_laundry_in_hall
,creb_laundry_in_kitchen
,creb_laundry_lower_level
,creb_laundry_laundry_room
,creb_laundry_multiple_locations
,creb_laundry_main_level
,creb_laundry_none
,creb_laundry_other
,creb_laundry_sink
,creb_laundry_upper_level
,creb_laundry_in_unit
,creb_laundry_washer_hookup
,lhamls_home_no_lot
,lhamls_model_not_for_sale
,lhamls_to_be_built

,sabor_restrictions_building
,sabor_restrictions_call_broker
,sabor_restrictions_cannot_be_subdivided
,sabor_restrictions_easements
,sabor_restrictions_edwards_recharge_zn
,sabor_restrictions_farm_animals_allowed
,sabor_restrictions_horses_allowed
,sabor_restrictions_manufactured_hms_allowed
,sabor_restrictions_mobile_homes_allowed
,sabor_restrictions_no_farm_animals
,sabor_restrictions_no_horses
,sabor_restrictions_no_mnfct_homes
,sabor_restrictions_no_mobile_homes
,sabor_restrictions_of_record
,sabor_restrictions_other
,sabor_restrictions_use_restrictions
,sabor_restrictions_not_applicable_none
,oabormls_laundry_basement
,oabormls_laundry_fourth_floor
,oabormls_laundry_lower_above_grade
,oabormls_laundry_lower_below_grade
,oabormls_laundry_main_floor
,oabormls_laundry_none
,oabormls_laundry_second_floor
,oabormls_laundry_third_floor
,oabormls_primary_bedroom_basement
,oabormls_primary_bedroom_fourth_floor
,oabormls_primary_bedroom_lower_above_grade
,oabormls_primary_bedroom_lower_below_grade
,oabormls_primary_bedroom_main_floor
,oabormls_primary_bedroom_none
,oabormls_primary_bedroom_second_floor
,oabormls_primary_bedroom_third_floor
,tarmls_exterior_barbecue
,tarmls_exterior_built_in_barbecue
,tarmls_exterior_courtyard
,tarmls_exterior_fountain
,tarmls_exterior_front_faces
,tarmls_exterior_gray_water_system
,tarmls_exterior_greenhouse
,tarmls_exterior_kennel_dog_run
,tarmls_exterior_misting_system
,tarmls_exterior_native_plants
,tarmls_exterior_only_native_plants
,tarmls_exterior_outdoor_kitchen
,tarmls_exterior_play_equipment
,tarmls_exterior_pond_on_lot
,tarmls_exterior_putting_green
,tarmls_exterior_rain_barrel_cistern_s
,tarmls_exterior_see_remarks
,tarmls_exterior_shed_s
,tarmls_exterior_waterfall
,tarmls_exterior_workshop
,mirs_structure_bi_level
,mirs_structure_condo_apt_first_floor
,mirs_structure_condo_apt_lower
,mirs_structure_condo_ranch_first_floor
,mirs_structure_condo_ranch_second_floor_above
,mirs_structure_one_half_story
,mirs_structure_one_story
,mirs_structure_plus_two_stories
,mirs_structure_two_story
,mirs_water_features_all_sports_lake
,mirs_water_features_association_access
,mirs_water_features_beach_access
,mirs_water_features_beach_facility
,mirs_water_features_beach_front
,mirs_water_features_canal_frontage
,mirs_water_features_commons_to_waterfront
,mirs_water_features_creek_stream_brook
,mirs_water_features_dock_pier_facility
,mirs_water_features_great_lake
,mirs_water_features_interior_lake
,mirs_water_features_island
,mirs_water_features_lake_frontage
,mirs_water_features_lake_river_access
,mirs_water_features_no_gas_motors
,mirs_water_features_no_wake_lake
,mirs_water_features_none
,mirs_water_features_pond
,mirs_water_features_river_frontage
,mirs_water_features_sandy_bottom
,mirs_water_features_sea_wall
,mirs_water_features_shared_waterfront
,mirs_water_features_water_view
,mirs_water_features_waterfront
,mirs_business_agriculture
,mirs_business_apartment_bldg
,mirs_business_auto_dealer_service
,mirs_business_bakeries
,mirs_business_bar_tavern
,mirs_business_beauty_barber_shop
,mirs_business_car_wash
,mirs_business_convenience_store
,mirs_business_dry_cleaner
,mirs_business_educational
,mirs_business_fast_food
,mirs_business_florist_nursery
,mirs_business_food_service
,mirs_business_hardware_store
,mirs_business_health_club
,mirs_business_hotel_motel
,mirs_business_ice_cream
,mirs_business_industrial_heavy
,mirs_business_industrial_medium
,mirs_business_industrial_light
,mirs_business_manufacturing
,mirs_business_marina
,mirs_business_medical_dental
,mirs_business_mobile_home_park
,mirs_business_office
,mirs_business_parking
,mirs_business_party_store
,mirs_business_presently_operating
,mirs_business_recreation
,mirs_business_religious
,mirs_business_residential
,mirs_business_restaurant
,mirs_business_retail
,mirs_business_service
,mirs_business_shopping_center
,mirs_business_single_use
,mirs_business_spa
,mirs_business_tree_farm
,mirs_business_warehouse
,mirs_business_wholesale
,lhamls_not_home_no_lot
,nnrmls_foundation_eight_point
,nnrmls_foundation_brick_mortar
,nnrmls_foundation_concrete_perimeter
,nnrmls_foundation_crawl_space
,nnrmls_foundation_full_perimeter
,nnrmls_foundation_insulated_foundation
,nnrmls_foundation_none
,nnrmls_foundation_other
,nnrmls_foundation_pillar_post_pier
,nnrmls_foundation_raised
,nnrmls_foundation_slab
,nnrmls_foundation_stone
,nnrmls_foundation_strip
,nnrmls_foundation_wood
,hmls_lot_zero_lot_line
,hmls_lot_wooded
,hmls_lot_streams
,hmls_lot_sprinkler_in_ground
,hmls_lot_springs
,hmls_lot_ponds
,hmls_lot_other
,hmls_lot_on_golf_course
,hmls_lot_many_trees
,hmls_lot_level
,hmls_lot_land_lease
,hmls_lot_lake_on_lot
,hmls_lot_lake_front
,hmls_lot_estate_lot
,hmls_lot_cul_de_sac
,hmls_lot_corner_lot
,hmls_lot_city_lot
,hmls_lot_city_limits
,hmls_lot_adjoin_greenspace
,hmls_lot_adjoin_golf_green
,hmls_lot_adjoin_golf_fairway
,hmls_lot_acreage
,paar_land_restrict_mfg_only
,paar_land_restrict_sb_only
,paar_land_restrict_mfg_or_sb
,lhamls_apx_garage_zero
,lhamls_apx_garage_twenty_two
,lhamls_apx_garage_twenty_eight
,lhamls_apx_garage_thirty_three
,lhamls_apx_garage_thirty_seven
,lhamls_apx_garage_forty_one
,lhamls_apx_garage_forty_six
,lhamls_apx_garage_fifty_one
,lhamls_apx_garage_sixty_one
,lhamls_apx_garage_seventy_one
,lhamls_apx_garage_over_eighty
,paar_pets_allowed_domestics
,paar_pets_allowed_farm_animals
,paar_pets_allowed_horses
,paar_water_source_private
,paar_utilities_propane
,paar_utilities_solar
,paar_utilities_hauled_water
,paar_water_source_shared_well




from stage.direct_idx_attribute_custom_4 s
join stage.etl_direct_idx_insert_listings t
	on s.source_listing_id = t.source_listing_id and s.source_id = t.source_id
	and s.batch_id = t.batch_id
where s.source_id in {}  and t.target_listing_id is not NULL;
"""

attribute_query_dict = {
    "listing_attribute": LISTING_ATTRIBUTE_QUERY,
    "listing_attribute_2": LISTING_ATTRIBUTE_QUERY_2,
    "listing_attribute_3": LISTING_ATTRIBUTE_QUERY_3,
    "listing_attribute_custom": LISTING_ATTRIBUTE_CUSTOM_QUERY,
    "listing_attribute_custom_2": LISTING_ATTRIBUTE_CUSTOM_QUERY_2,
    "listing_attribute_custom_3": LISTING_ATTRIBUTE_CUSTOM_QUERY_3,
    "listing_attribute_custom_4": LISTING_ATTRIBUTE_CUSTOM_QUERY_4,
}
