{% macro get_vendor_names(vendorid) -%}
case 
    when {{ vendorid }} = 1 then 'Creative Mobile Tech'
    when {{ vendorid }} = 2 then 'VeriFone Inc.'
    else 'Unknown'
end

{% endmacro %}