<?php


function tools_more_tools()
{
    $tools = [
        [
            "name" => "Fuel Price",
            "icon" => "fas fa-gas-pump", // Font Awesome icon for fuel
            "link" => home_url() . "/tools/fuel-price"
        ],
        [
            "name" => "Car Comparison",
            "icon" => "fas fa-exchange-alt", // Font Awesome icon for car comparison
            "link" => home_url() . "/compare-cars"
        ],
        [
            "name" => "Car Filter",
            "icon" => "fas fa-car", // Font Awesome icon for car filter
            "link" => home_url() . "/cars"
        ],
        [
            "name" => "Ranking",
            "icon" => "fas fa-trophy", // Font Awesome icon for ranking
            "link" => home_url() . "/car-ranking"
        ],
        [
            "name" => "Carpedia",
            "icon" => "fas fa-book", // Font Awesome icon for Carpedia
            "link" => home_url() . "/carpedia"
        ],
        [
            "name" => "Car Issue",
            "icon" => "fas fa-exclamation-circle", // Font Awesome icon for car issue
            "link" => home_url() . "/tools/car-problems"
        ]
    ];


    ob_start();
?>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <h2 class="wa-title-text">More Tools</h2>
    <div class="container-tools">
        <div class="tools-grid">
            <?php foreach ($tools as $tool) : ?>
                <div class="tool-item">
                    <a href="<?php echo $tool['link']; ?>" class="tool-link">
                        <i class="<?php echo $tool['icon']; ?>"></i>
                        <p><?php echo $tool['name']; ?></p>
                    </a>
                </div>
            <?php endforeach; ?>
        </div>
    </div>


    <style>
        .container-tools {
            max-width: 100%;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            border-radius: 5px;



        }


        h2 {

            margin-bottom: 20px;

        }


        .tools-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            /* 2 columns layout */


        }


        .tool-item {
            background: white;


            align-items: center;
            border-bottom: 1px solid #f0f0f0;
            border-right: 1px solid #f0f0f0;
            box-sizing: border-box;
            cursor: pointer;
            padding: 14px 12px;
        }


        .tool-link {
            text-decoration: none;
            color: #262626;
            display: flex;
            flex-direction: column;
            align-items: center;
            font-size: 12px;
        }


        .tool-link i {
            font-size: 40px;
            width: 40px;
            height: 40px;
            color: #262626;
            margin-bottom: 10px;
        }


        .tool-link p {
            margin: 0;
            color: #262626;
            font-size: 12px;
        }
    </style>


<?php
    return ob_get_clean();
}


add_shortcode('tools_more_tools', 'tools_more_tools');
