something = '''(SELECT
(CASE when c.tenantName in ("sdbkidsstop") then "Kid's Stop Dentistry"
when c.tenantName in ("sdbsaginaw") then "xx"
when c.tenantName in ("brentwoodpd") and c.clinicID = 1 then "Brentwood Pediatric Dentistry"
when c.tenantName in ("brentwoodpd") and c.clinicID = 2 then "Nolensville Pediatric Dentistry"
when c.tenantName in ("sdbjustkidsteeth") then "Just 4 Kids Teeth"
when c.tenantName in ("sdbpedsdentalcare") then "Pediatric Dental Care"
when c.tenantName in ("sdbchildrens") then "Children's Dental & Orthodontics"
when c.tenantName in ("sdbcolleyville") then "Colleyville Childrens Dental"
when c.tenantName in ("drrhondahogan") then "Rhonda Hogan Pediatric and Adolescent Dentistry"
when c.tenantName in ("sdbknoxville") and c.clinicID = 1 then "Knoxville Pediatric Dentistry"
when c.tenantName in ("sdbknoxville") and c.clinicID = 3 then "Knoxville Pediatric Dentistry"
when c.tenantName in ("sdbknoxville") and c.clinicID = 5 then "Knoxville Pediatric Dentistry"
when c.tenantName in ("sdbknoxville") and c.clinicID = 2 then "Knoxville Pediatric Dentistry"
else "No Office" end) as Office,(CASE when c.tenantName in ("sdbkidsstop") then "KSTP"
when c.tenantName in ("sdbsaginaw") then "??"
when c.tenantName in ("brentwoodpd") and c.clinicID = 1 then "CBNT"
when c.tenantName in ("brentwoodpd") and c.clinicID = 2 then "CNOL"
when c.tenantName in ("sdbjustkidsteeth") then "J4KT"
when c.tenantName in ("sdbpedsdentalcare") then "PDSF"
when c.tenantName in ("sdbchildrens") then "PCAM"
when c.tenantName in ("sdbcolleyville") then "COLL"
when c.tenantName in ("drrhondahogan") then "GPED"
when c.tenantName in ("sdbknoxville") and c.clinicID = 1 then "KXPF"
when c.tenantName in ("sdbknoxville") and c.clinicID = 3 then "KXPT"
when c.tenantName in ("sdbknoxville") and c.clinicID = 5 then "KXPB"
when c.tenantName in ("sdbknoxville") and c.clinicID = 2 then "Closed"
else "xx" end) as Abbreviation,(CASE when c.tenantName in ("sdbkidsstop") then "4008"
when c.tenantName in ("sdbsaginaw") then "??"
when c.tenantName in ("brentwoodpd") and c.clinicID = 1 then "6002.1"
when c.tenantName in ("brentwoodpd") and c.clinicID = 2 then "6002.2"
when c.tenantName in ("sdbjustkidsteeth") then "6003"
when c.tenantName in ("sdbpedsdentalcare") then "2009.6.1"
when c.tenantName in ("sdbchildrens") then "4000"
when c.tenantName in ("sdbcolleyville") then "4006.1"
when c.tenantName in ("drrhondahogan") then "7002"
when c.tenantName in ("sdbknoxville") and c.clinicID = 1 then "6004.3.2"
when c.tenantName in ("sdbknoxville") and c.clinicID = 3 then "6004.3.4"
when c.tenantName in ("sdbknoxville") and c.clinicID = 5 then "6004.3.1"
when c.tenantName in ("sdbknoxville") and c.clinicID = 2 then "Closed"
else "xx" end) as GL_Location_ID,(case
 when c.tenantName in ("brentwoodpd","sdbkidsstop","sdbjustkidsteeth","sdbpedsdentalcare",'sdbchildrens','sdbcolleyville','sdbsaginaw','drrhondahogan','sdbknoxville') then "PEDs"
 else "Unknown" end) as Specialty,
d.Provider,
(CASE when account in ("Revenue") then "Production"
else "xx" end) as Category,
(case
 when c.tenantName in ("brentwoodpd","sdbkidsstop","sdbjustkidsteeth","sdbpedsdentalcare",'sdbchildrens','sdbcolleyville','sdbsaginaw','drrhondahogan','sdbknoxville') then "Curve"
 else "Unknown" end) as pms_name,
date(postedOn) as Date_of_Service,
(Case when c.providerId is null then 0 else Cast(count(distinct patientId) as float64) end) as PatientsSeen,
(Case when c.providerId is null then 0 else Cast(count(distinct postedOn) as float64) end) as DrDays,
Cast(count(insuranceCode) as float64) as BillableCharges,
Cast(ROUND(sum(calcProduction),2) as float64) as Production,
d.Provider_Type
FROM `specialty-dental-brands.business_intelligence.CurveAPIV2` c
left JOIN `specialty-dental-brands.business_intelligence.Curve_API_NormalizedDoctorNames` d on d.tenantName = c.tenantName and d.providerId = c.providerId
WHERE account = 'Revenue' and calcProduction > 0
GROUP BY c.tenantName,d.Provider,account,postedOn,d.Provider_Type, c.providerId, c.clinicId )'''