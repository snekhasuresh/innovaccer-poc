<?php
function enqueue_add_car_css()
{
    wp_enqueue_style('add-car', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/overview/car/css/add-car.css', array(), '1.0', 'all');
}
// Define the shortcode function
function car_listing_template_shortcode()
{
    enqueue_add_car_css();
    ob_start(); // Start output buffering
?>
    <div class="card">
        <div class="logo">CARSOME</div>
        <div class="header-card">Get a deal on your trade in within 24 hours!</div>

        <div class="content-card">
            <div class="image-cont">
                <img src="https://images.wapcar.my/file1/2a7736cdf1bc48d799e8200c28296c55_360x240.jpg" class="image-card">
            </div>
            <div class="car-info">

                <div class="car-model-side-widget">2022 Honda HR-V 1.5 S</div>
            </div>

            <div class="upgrade-text">Upgrade</div>

            <div class="add-car-box" id="add-car-box">
                <div class="plus-icon">+</div>
                <div>Add your car</div>
            </div>

            <button class="check-price-btn">Check Your Car Price</button>
        </div>

        <div class="footer">
            Not trading in? <a href="#">Sell your car ›</a>
        </div>
    </div>
    <!-- -------------------------------------------------------------------------------------------------------- -->
    <div class="dropdown" id="dropdown" style="display: none;">
        <div class="alphabet-sidebar" id="alphabetSidebar">
        </div>

        <div class="brand-list" id="brandList">
        </div>
    </div>
    <script>
        document.addEventListener("DOMContentLoaded", function() {
            // Get the 'Add your car' button and the dropdown
            const addCarBtn = document.getElementById('add-car-box');
            const brandDropdown = document.getElementById('dropdown');

            // Toggle dropdown visibility on 'Add your car' click
            addCarBtn.addEventListener('click', function() {
                if (brandDropdown.style.display === "none" || brandDropdown.style.display === "") {
                    brandDropdown.style.display = "flex"; // Show dropdown
                } else {
                    brandDropdown.style.display = "none"; // Hide dropdown
                }
            });

            // Define car brands with alphabetically categorized brands
            const carBrands = {
                A: ["Audi", "Aston Martin", "Acura"],
                B: ["BMW", "Bentley", "Bugatti"],
                C: ["Chevrolet", "Citroen", "Cadillac"],
                D: ["Dodge", "Daihatsu", "Datsun"],
                E: ["Ferrari", "Fiat", "Ford"],
                F: ["Ford", "Fiat", "Ferrari"],
                G: ["Genesis", "GMC"],
                H: ["Honda", "Hyundai"],
                I: ["Infiniti", "Isuzu"],
                J: ["Jaguar", "Jeep"],
                K: ["Kia", "Koenigsegg"],
                L: ["Lamborghini", "Land Rover", "Lexus"],
                M: ["Maserati", "Mazda", "McLaren"],
                N: ["Nissan"],
                O: ["Opel"],
                P: ["Peugeot", "Porsche"],
                Q: ["Qoros"],
                R: ["Renault", "Rolls-Royce"],
                S: ["Subaru", "Suzuki", "Saab"],
                T: ["Tesla", "Toyota"],
                U: ["Ultima"],
                V: ["Volkswagen", "Volvo"],
                W: ["Wiesmann"],
                X: ["Xpeng"],
                Y: ["Yamaha"],
                Z: ["Zenos"]
            };

            // Generate Alphabet Sidebar
            const alphabetSidebar = document.getElementById("alphabetSidebar");
            const brandList = document.getElementById("brandList");

            Object.keys(carBrands).forEach(letter => {
                // Create alphabet sidebar link
                const alphabetLink = document.createElement("a");
                alphabetLink.href = `#${letter}`;
                alphabetLink.textContent = letter;
                alphabetSidebar.appendChild(alphabetLink);

                // Create brand section for each letter
                const brandSection = document.createElement("span");
                brandSection.classList.add("alpha-section");
                brandSection.id = letter;
                brandSection.textContent = letter;
                brandList.appendChild(brandSection);

                // Add brands for each letter
                carBrands[letter].forEach(brand => {
                    const brandItem = document.createElement("div");
                    brandItem.classList.add("brand-item");

                    const brandSpan = document.createElement("span");
                    brandSpan.textContent = brand;

                    brandItem.appendChild(brandSpan);
                    brandList.appendChild(brandItem);
                });
            });

            // Function to highlight the current active letter in the sidebar based on scroll
            const alphabetLinks = document.querySelectorAll('.alphabet-sidebar a');
            const brandSections = document.querySelectorAll('.alpha-section');

            brandList.addEventListener('scroll', function() {
                let activeLetter = null;

                brandSections.forEach(function(section) {
                    const sectionTop = section.getBoundingClientRect().top;
                    const brandListTop = brandList.getBoundingClientRect().top;

                    // If section is about to enter view, highlight its letter
                    if (sectionTop - brandListTop < 50 && sectionTop - brandListTop > -50) {
                        activeLetter = section.id;
                    }
                });

                if (activeLetter) {
                    alphabetLinks.forEach(function(link) {
                        if (link.textContent === activeLetter) {
                            link.classList.add('active');
                        } else {
                            link.classList.remove('active');
                        }
                    });
                }
            });
        });
    </script>
<?php
    return ob_get_clean();
}

add_shortcode('add-new-car', 'car_listing_template_shortcode');
