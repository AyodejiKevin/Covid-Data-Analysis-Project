USE covid_portfolio_project;

CREATE TABLE Covid_Vaccination (
iso_code VARCHAR(10),
continent VARCHAR(50),
location VARCHAR(100),
date DATE,
total_tests VARCHAR(100),
new_tests VARCHAR(100),
total_tests_per_thousand VARCHAR(100),
new_tests_per_thousand VARCHAR(100),
new_tests_smoothed VARCHAR(100),
new_tests_smoothed_per_thousand VARCHAR(100),
positive_rate VARCHAR(100),
tests_per_case VARCHAR(100),
tests_units VARCHAR(50),
total_vaccinations VARCHAR(100),
people_vaccinated VARCHAR(100),
people_fully_vaccinated VARCHAR(100),
total_boosters VARCHAR(100),
new_vaccinations VARCHAR(100),
new_vaccinations_smoothed VARCHAR(100),
total_vaccinations_per_hundred VARCHAR(100),
people_vaccinated_per_hundred VARCHAR(100),
people_fully_vaccinated_per_hundred VARCHAR(100),
total_boosters_per_hundred VARCHAR(100),
new_vaccinations_smoothed_per_million VARCHAR(100),
new_people_vaccinated_smoothed VARCHAR(100),
new_people_vaccinated_smoothed_per_hundred VARCHAR(100),
stringency_index VARCHAR(100),
median_age VARCHAR(100),
aged_65_older VARCHAR(100),
aged_70_older VARCHAR(100),
gdp_per_capita VARCHAR(100),
extreme_poverty VARCHAR(100),
cardiovasc_death_rate VARCHAR(100),
diabetes_prevalence VARCHAR(100),
female_smokers VARCHAR(100),
male_smokers VARCHAR(100),
handwashing_facilities VARCHAR(100),
hospital_beds_per_thousand VARCHAR(100),
life_expectancy VARCHAR(100),
human_development_index VARCHAR(100),
population VARCHAR(100),
excess_mortality_cumulative_absolute VARCHAR(100),
excess_mortality_cumulative VARCHAR(100),
excess_mortality VARCHAR(100),
excess_mortality_cumulative_per_million VARCHAR(100));

LOAD DATA INFILE 'Covid_Deaths.csv' INTO TABLE covid_deaths 
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

CREATE TABLE covid_deaths (
iso_code TEXT,
continent TEXT, 
location TEXT, 
date DATE, 
population VARCHAR (100), 
new_cases VARCHAR (100),
new_cases_smoothed VARCHAR (100), 
total_deaths VARCHAR (100), 
new_deaths VARCHAR (100), 
new_deaths_smoothed	VARCHAR (100), 
total_cases_per_million VARCHAR (100), 
new_cases_per_million VARCHAR (100), 
new_cases_smoothed_per_million VARCHAR (100), 
total_deaths_per_million VARCHAR (100), 
new_deaths_per_million VARCHAR (100), 
new_deaths_smoothed_per_million VARCHAR (100), 
reproduction_rate VARCHAR (100), 
icu_patients VARCHAR (100), 
icu_patients_per_million VARCHAR (100),
hosp_patients VARCHAR (100), 
hosp_patients_per_million VARCHAR (100), 
weekly_icu_admissions VARCHAR (100), 
weekly_icu_admissions_per_million VARCHAR (100), 
weekly_hosp_admissions VARCHAR (100), 
weekly_hosp_admissions_per_million VARCHAR (100), 
total_tests VARCHAR (100)); 

"C:\ProgramData\MySQL\MySQL Server 8.0\Data\covid_portfolio_project\Covid_Deaths.csv"

Checking Data Integrity 

SELECT *
FROM covid_deaths
Order by 3,4; 

SELECT *
FROM covid_vaccination 
Order by 3,4; -- Order by columns 3

-- We Shall Select The Relevant Data For Use. 

SELECT location, date, total_cases_per_million, new_cases, total_deaths, population 
FROM covid_deaths 
Order by 1,2; 

-- Looking At Total_Cases vs Total Deaths. We do not have the total_cases column in our dataset so we need a way to derive it. 

-- A formula to do that is this: (total_cases_per_million * population) / 1000000 AS total_deaths. Now that we have derived new cases,
-- we shall now update our table. 

ALTER TABLE covid_deaths 
ADD column total_cases VARCHAR(100);

UPDATE covid_deaths 
SET total_cases = (total_cases_per_million * Population) / 1000000; 

-- Now, let's consider total cases vs total deaths 
-- It shows the likelihood of dying if you contract Covid in your country 

SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS Death_Percentage 
FROM covid_deaths
WHERE location LIKE '%states%'
GROUP BY continent 
Order By 1,2; 

-- Looking At The Total Cases vs The Population 
-- Shows what percentage of population got Covid. 

SELECT location, date, total_cases, population, (total_cases/population) * 100 AS Death_Percentage 
FROM covid_deaths 
-- WHERE location like '%states%'
Order By 1,2; 

-- What country has the highest infection rate compared to population 

SELECT location, population, MAX(total_cases) AS highest_infection_count, MAX((total_cases/population)) * 100 AS
		percent_population_infected 
FROM covid_deaths 
GROUP BY continent, population  
Order By percent_population_infected DESC;

-- showing countries with the highest death count per population

SELECT location, MAX(cast(total_deaths AS UNSIGNED)) AS Total_Death_Count 
FROM covid_deaths 
GROUP BY continent  
ORDER BY Total_Death_Count DESC; 

-- Let's break things down by continent 

SELECT continent, MAX(cast(total_deaths AS UNSIGNED)) AS Total_Death_Count 
FROM covid_deaths 
GROUP BY continent 
ORDER BY Total_Death_Count DESC; 

 -- Global Numbers 
 
 -- Global Death Percentage 
 SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths) / SUM(new_cases) * 100 AS Death_Percentage 
 FROM covid_deaths 
 -- WHERE location like '%states%' 
 WHERE continent IS NOT NULL 
 GROUP BY date
 ORDER BY 1,2; 
 
 SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths) / SUM(new_cases) * 100 AS Death_Percentage 
 FROM covid_deaths 
 -- WHERE location like '%states%' 
 WHERE continent IS NOT NULL 
 ORDER BY 1,2;

-- Exploring Covid_Vaccination Table 

-- Joining the covid_deaths and covid_vaccination tables now... 

SELECT *
FROM covid_deaths dea
JOIN covid_vaccination vac
ON dea.location = vac.location 
	AND dea.date = vac.date;
    
-- Looking at total_population vs total_vaccinated. 
SELECT dea.continent, dea.location, dea.date, dea.population, 
		vac.new_vaccinations
FROM covid_deaths dea
JOIN covid_vaccination vac
ON dea.location = vac.location 
	AND dea.date = vac.date 
WHERE dea.continent is not null 
ORDER BY 1,2; 

-- Looking at new vaccinations per day // Rolling count 
SELECT dea.continent, dea.location, dea.date, dea.population, 
		vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
        AS Rolling_People_Vaccinated -- it keeps the sum running through only through a location 
FROM covid_deaths dea
JOIN covid_vaccination vac
ON dea.location = vac.location 
	AND dea.date = vac.date 
WHERE dea.continent is not null 
ORDER BY 1,2; 

-- Total Population vs Vaccinations 
-- We can't use a new column we've just created for calculations so we'll do something else // Create a temp. table 
-- USE CTE 
WITH pops_vs_vac (continent, location, date, population, new_vaccinations, Rolling_People_Vaccinated) 
AS (
SELECT dea.continent, dea.location, dea.date, dea.population, 
		vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
        AS Rolling_People_Vaccinated -- it keeps the sum running through only through a location 
        -- (Rolling_People_Vaccinated/population) * 100
FROM covid_deaths dea
JOIN covid_vaccination vac
ON dea.location = vac.location 
	AND dea.date = vac.date 
WHERE dea.continent is not null
-- ORDER BY 2,3);

-- Temp Table 

create TABLE percent_population_vaccinated 
( continent nvarchar(255), 
  location nvarchar (255), 
  DATE datetime, 
  Population numeric, 
  new_vaccination nvarchar(255),
  rolling_people_vaccinated nvarchar(255))
  
INSERT INTO percent_population_vaccinated (
	SELECT dea.continent, dea.location, dea.date, dea.population, 
		vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
        AS Rolling_People_Vaccinated -- it keeps the sum running through only through a location 
        -- (Rolling_People_Vaccinated/population) * 100
FROM covid_deaths dea
JOIN covid_vaccination vac
ON dea.location = vac.location 
	AND dea.date = vac.date)
WHERE dea.continent is not null
-- ORDER BY 2,3);

SELECT *, (Rolling_People_Vaccinated/Population) * 100 
FROM percent_population_vaccinated; 

-- Creating view to store data for later visualizations 

CREATE VIEW percent_population_vaccinate AS 
SELECT dea.continent, dea.location, dea.date, dea.population, 
		vac.new_vaccinations, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date)
        AS Rolling_People_Vaccinated -- it keeps the sum running through only through a location 
        -- (Rolling_People_Vaccinated/population) * 100
FROM covid_deaths dea
JOIN covid_vaccination vac
ON dea.location = vac.location 
	AND dea.date = vac.date;
WHERE dea.continent is not null
-- ORDER BY 2,3);
