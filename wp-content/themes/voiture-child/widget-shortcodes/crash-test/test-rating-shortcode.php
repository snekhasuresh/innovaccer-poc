<?php

function enqueue_crash_rating_css()
{
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'brands_list')) {
        wp_enqueue_style(
            'crash-rating-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/crash-test/css/test-rating.css',
            array(),
            '1.0',
            'all'
        );
//     }
}
add_action('wp_enqueue_scripts', 'enqueue_crash_rating_css');

function crash_rating_shortcode() {
    enqueue_crash_rating_css();
	  $star_rating = wp_get_attachment_image_url(356097, 'starrating');


    $icons = [
        'starrating' => $star_rating,
     
    ];
    ob_start(); // Start output buffering
    ?>
    <div class="container-ratting">
        <!-- Badge Section -->
        <div class="badge-section">
			 <img src="http://34.126.82.224/wp-content/uploads/2025/02/a280336259b74a6f944d552865fbd2d3_190.png" class="badge" />
            <div class="badge-text">
                <a href="#">ดูรายงานฉบับเต็ม &gt;</a>
            </div>
        </div>


        <!-- Ratings Section -->
        <div class="ratings-section" id="ratings-container">
            <!-- Dynamic Ratings will be inserted here -->
        </div>
    </div>


    <div class="footer">ให้บริการโดย ASEAN NCAP</div>


    <script>
        document.addEventListener("DOMContentLoaded", function () {
          const ratingData = [
            {
              value: 36.59,
              color: "#2D79C7",
              icon: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFQAAABUBAMAAADuRQ3yAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAnUExURRU1XUdwTBIzWhQ1XRQ0XRQ2XBQ0WxU1XRU1XBU1XRQ1XRU1XRU1XeP83TwAAAANdFJOU/8ADezXMh9+Z03CrZeEWQGgAAADmklEQVRIx+3Xy08TURQH4JPcSVR005vYSOLmTtIB4maSNkEKC5JWpdCNTaAluplJLG3cgOEtC3mUh7ABA/XBwqQoIC5UXhVYgAJi4Y/y3N6Bqp3X2nhXtPlymTn3nN9MweN6wX/650dZll3SxIa/ZYC5ob6nIZD22tzQhAoAZMAF9T4HvqK6M/V9L9KKBWeaelukUo8z7TDokTONNwq66YK63zXlniqfRAXGnSntFnWNuaCLxdPKMxcH+6iAtmXBTQ/Q7Ia/+cRVZ3mS6cn5pMvW9r1qY+6o92XflOaKKkM74bUfuhuazmGxAvOlS6Ays6DdvK4Q7L34ovLpE4uDFa1NtheMvWhV7hszpWnRLkBWjVujj2HLhLZnWMc5DW5PteLSrx9KZhcwkb9v9Cu/tb0+XJlqtWG2nNKlYG/qghI/rkBLSm3STKhKtkpULCnuf8fMKLyvEa0N/vO9s4FjkyOgIbjjE5REB0PFP4Jxf3TE7FrhjjEwZLVjJ8Qvdi2ukk29vAIFiOj7gkaokuTFevVhCQI9rGzXQWjSjNOKiKBNz7TMrJOTMuqZhtr7YgxJHf/M2MPQsvJaXS6nM9AwK5IQ6sQ3L0jGUzMxX04XoX54Wv2djgZwyn3lPUA7IDi8KOhlI5f6kVGTdqmGQFf2D+prZeZTUA3kIGFQ2X5gbqhwkDYos6eVIfjsE/RSUpFlZkPXYUsRVNrd3d1esKbXD2FTEbFNwuFw85EN7YbVZO6iV8ltGzoGK/pGqa8v6ZbU+xqaYoUSvTav2dBo7M1vdHLWiuLM188ldsLFhVNwdSijWdFbQEa8CT7Uff2HKlScnc5axdtNleR1pfURrtY3jSBtHw5Y9IDnRg4fA7rMF8PwIMv7y5oF9Y4BCazt8pOaw0QgK/tSxoLSh41o+U0192LQkUgnyWsWUVyzf16ouzwRIp3QELOgcqIgokI69nL6DKSvzOqFJCvKGh2h+A/qRgFqY1YPTiXZVlyMYiLcxrivOHZ+e8NEqB1UsQyaE6WYCA1D2L3BYcddMRHqB9axbY9Y+5z9rpgIwXGeSyttfT32u2IiBLracdSi/aEr9jSOt3Tgw6G4d1YMRhvKw+OATuMFFC5eZSwoD48vLH36sT+X1+0pD48t5s3+nFgadyiWF2v6TvPUnJU2taSYCJuMVi01Z5yOgG7wC6APYFVzpItqcAQf8nvjzJGmQvdwYCunHNvlr1eRf/rnzi/P85+KXxm+QgAAAABJRU5ErkJggg==",
              description: "การปกป้องผู้โดยสารที่เป็นผู้ใหญ่ (AOP)",
            },
            {
              value: 18.32,
              color: "#D15B2E",
              icon: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFQAAABUBAMAAADuRQ3yAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAnUExURYExFEdwTIMvEIExE4EwEYExE4IxE4ExE4ExE4ExE4ExFIExE4ExE0vHGE0AAAANdFJOU/8ACvIb4TJks4BImsw2DQQmAAAEHElEQVRIx+2Xy2vbWBSHLwhDB7LRRbiQ3RUiaTIbgRAdqDcWxmndLgLGE7fxRsE4sbtq6zydRSF9+NFFm5nm5WxSgj1OZjHTJM3DXQSCSB/5o3rulSzHlm6cVVfVxrb0cXTO77yukXjtC/1CO39iWZbJtVDcsLTUiXENFEercaSXtklvVFmMCwjp50ZvNLeH4BLG873R3VOKosCL3uj0KEPRN6MXKr+1STQ20QsdaKEP873Q6H8OesdG8TWsMglwfcHgoorrwAf6q1qqGdyw/lIvhTV5pidXeChZtMUSymBNKaio74Sra/Y5Q/v+pu8/uBKNbVIPBOqq3FhC6MYMvwizNLOBMlTWABhFv/mEFYlE2N3Y2oV1UQJbSmNJRcK5D5rZ2dlmoseqzeNt0D72Hoz2rXh1xc1iscwsKJkMiCnl1s7A51LaB7XAL9usKIuyHCtQn/UNn8TiNRXdYG/DmdxOvfomoVKjhl8NzKso9Io11+z8evUNy0Rog/ihMQj3ERSSshnX4KKkdmj4FmEUorgNj6JLTsEgvTJDfFHpJRQp5Cj2vIWOzxj+pS19hizm26iQXJ0gHHQOBN9vo/ong9cwOAuWQALFaRl9n3B7axCE/JOIeM9BP/LbsB8keLglinN2HwivwnyUSvAEJmFraJhcNLgJ6VmGhnLQr1tclEpAHYydOrLyUXwfnoODNBcsrnd8dAgSf26yGmPXR8IdRDfPmIO44KA+ErjoAUJJkGDSQV+bXDS4CM/BweFRblwtVJqC5/thMWjZaGqLcIfmXYirbLIaY/l6SlGML20xFx2y45IcCYQVGpeUq2950UEnrl0nrkMTy0q0cbRMPCiNS18W8YgT13dTqlcvEvGy6UEViEuAMqUCs7jyuepL1a63LhTfstXsb8X1Ja5RX/T/vZNwKA5xmWJwrtWz9ofm5q2NDkJjJ0GCLOq8zk0P2v8eLIEEI2onCrJ0o8FpKjxUw2gn6tZjG8X/wv0TIvYfOIyWqqxDc6BPYc+AfwBvLoXduAS9spD+XW0X2SX0PjRLMgzOUjlDlfWdTEQeorF+8KAjMFoCcDtYsBKJymqNtGLNk26UrtgADFU5MrlQS0dkVpsQgHAY7kaVadUZ1VLaHVlToMd3Dyr+A7dDnauCeRWa8KCPT+muiLjbme6yYZhigRkPOgxzUPjDXZbSLHwlz8CrQ+I5D9DNrbNTAFZy9fliGUKbOoUiI/6bO1CqybICxcp2LL63B8nwotE5MCukis1ms3gRtzd3dPHL8azP5m7YLSBodm77yuDK4+Oa7IMqhY4KFG7TXRYxiN+hNGtpLqjpiTHCP+oqk5aLpkoL6asO0ANHFm0/sJiqrKbJVShR6kcWLOfK6rbRdZT2HMtxtL7ebB7P1sK//pj8HPQHAk6uCsN0YlkAAAAASUVORK5CYII=",
              description: "การปกป้องผู้โดยสารที่เป็นเด็ก (COP)",
            },
            {
              value: 18.16,
              color: "#8BC34A",
              icon: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFQAAABUBAMAAADuRQ3yAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAnUExURUdwTEVjAEVjAEVjAEZhAEVjAURgAEVjAERjAERjAERiAENhAEVgALK7uAgAAAANdFJOUwD/978phxbW5qVtVj90OBFfAAADcElEQVRIx+1WK5LjMBANMpduIKFwgSlzl9FSAQcZuHSBAQoOsXgqQAcw8dwgDhwWHWq79bMndjKDd0fAH/m59br7qdW73e/4f0bbtsUPYN3YaMO5Lsf6O2g1WMMJMeJQvQa6M+eUEkIo4bycXiCLuyA4mL+KsX3O88wRwrXQ4eHylG+FNjk3QgjwDJ7tM76VwVUPFUSqKNx4BMb6CdadwGYKUjUhif7zqdF+mj0p0K7ZNPvnBAveFhOTJmS/ZbYDo+KrxxVg+40gVGC0/BrITjCy32AAFsrHkBcQvXKdqCOhdjV7ZkSsUnZlW7Q6MDBuQPU65YUm9LAxtxXCOyNGraluacNBDNRq/eyUa8QlS9WuyAL0kkVrhc1Sbb6SVe4qMlUQLSWU6zqTFW72Qt0RWi9ECwmtMlnhXFrjbvh4Tlnp3sJuiaFTOwEbh+vIdzrRg+W2bZNECO6AkBBVCC4G1h+ypLicrqZX6j4Y2K9CIl4fJrXTWroRASpKilA5jae9UpPEFzGApIiRY1UcjXQDznmoZH7zcUInv/nopd0VDYMocD4xvGJd8Aya6AdAT/4u0/8wk7/h5E7EN2LDkw9wFatG/lguoTRB0fXuAUo99JjXZyvoPjMQm9CZAH2AnuNLP55mD2SymgzZBZT2GKtFsDCAJkMvy2C9t4W6S4MpkJgCyBaUL7EMVlyLvoNU3eBTKoUvrzc1r+lTMEToR64olM8qvLMFdAwv/ONBhHXS6wy9spkMYnVARmmrmOIlNO2g4i0wj5tCDezRLSuTWR/9XBQa2TxAqZVpx/qc5qJgm/D9kuXSS8Y/c/WANLXpPyp9Ek0sjBhKRlOBxvNin9lQzEr413tFR9CFjhXnnNQRqq6UnqxL0A+A9msoljLpY0BvkSqtgQeNjuG+mh/7Vnmy774we4fhboMv6HKIXGHBi50ySSL4BdRYYYWvY06or4TKMi8FicFCdgP362FCyypBb6HSBSkAWS69I+4Y9giY1VNIlw+c0ywccerU39JJxuuokx7rahegxT0dnMpol4pmqNFgFnqBSXmoGs/zcezUXIqj/BjarZCAqwYxH/JOrc5OG6tXuoqnrU7XcLIcvKxfdUQLpLi1r7osy5NhaJ7cy0arqMYzdCLclN+1ZBg4C+2Q0fb7Rs93j7sftY+/498YfwGyHlLnkEHtFgAAAABJRU5ErkJggg==",
              description: "เทคโนโลยีเพื่อความปลอดภัย (STAs)",
            },
            {
              value: 10.39,
              color: "#9066B8",
              icon: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFQAAABUBAMAAADuRQ3yAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAK7OHOkAAAAnUExURUMmaUdwTEMmaUMmaUImaUMmaUImakInaUImaUImaUMna0UrbEImaXm1xfwAAAANdFJOU/4A7Qio2EVhwJMVKHoc2bnBAAAEZklEQVRIx+2Xy2sqVxjAD4dIDXQjB1LQzWG4AwayGQZyQTchmOiAi2FwrgbupjSNMcmiFDPeRBdBTKI0i1C8Pm6zEYsF7SZMMY3JooREDckf1fMYG183zuZ2db+FRr4f3znnewe4bAv4iv5vqExEsoX6kpqwkbaFtisYxLp1G6i8jwGAb9I20NYdIOJYl2ejf6xQFHxTn41eH5Hzc48lG+i2StCSf9cOSl7lKMl1yabV4E1HsnNXiIGYD5qz0dtVXRew8H7Hhl8PqtXHNbBoB00GAkpAX7KBXpEQQOXxdPZd0Sb1QCxszPar7ycaVrjUnJku8u0RSwHvbGf5abCozKXlGeifq5wEcOPwddT9l0Y5QcfAWzJd6BV04YFZjMTXAPR+kl5BET8fhtt9DOBJp33T/Bzq2ebP35XffgRADK4VDOkzqHV+wXR5DrIARFRdMXblqeh3WYama8RrH5jXYGy9Pg31HIQoG9khp6KrvsqcERm0hBF0IVClzwrWWISvedxg1JxE3Qk9UlwFwhN7CmolmVng/E2aQD0V4FQqauz70Y4A3tXHUbSsASiePpRqg9T5meeDVTsMRU0mV7RZRftPHfqj7kK+O44uHr6gC/EUkUyFasRCj/5IEUtWPg6j6G0xQEVnigj7Wyk129xbI6j7bzAp88amRQ7fFU1DncqaleTgnfmC/o7BKwJ/GQrB8vNFtXg8nY8FLg6HUF97K54pasIUUsg9G526/GL1MaTHlNT9pF3luXgs5so0fyiK/HshDYg5I3M8Toph6myxYJgSQ923x/RoIXaxN262UNW46omj/iSGgiCQiopnxy4aJjaZat2kKLrFwJGrhshneHMU3aiSYsxViYNpYgJiFDjOekamiOHG5sgN4FkFi0ovnqmoIFwjaCML5g4RQuTb+ThyAxg8IsUjSxJJXK9J0LjqYK3Ml9Fg4WHk/KIq9mha+5Oqo+sCKIPn+Dz1ZMHi9jAa/RV7eaY0VuCFBFAILPGC8Nzj+R+H0ZNzGOaZ4tvHOYJqMMpbk3sPzw2jkKDd2mDuxQiKHetWbV3huf3hUAVXYNrqQwlVZGjJQhujqLc8jEKGdqeji6kVuGOhexY63ap4Gl+Bg96WUAX2rBOruYzcFb4/bJzDdIfrNlX6rBCMylYfwvPbgiaKuq7H8krXbJ3DsjnYZ6iz7vG3vDV7+mD+Op/L55Vq9cIgvpfvQOSSqZr/AEWi0XJ26eVRPAtPG1tc2jc0LB+wl3VtlDiCJLCuKwyjHbKv+e7/e+BAiIvCdaJq7WPHDk3CCnH2bqd9r4E3Y9O6tQrE8k2TNuXCJUHdpAg2elsZMn7Oxkawu49hwdhKkJ5bpvmKGmtkpJHNggyWsX0FtbNUpWHW80lteZK8AYiFiQnsrzAVFIKXvLiX+5jNtZ2JJQi1WSOFwR94xbp8e8VQPqcYtcmt059hKjqdeHeRWvGU0ZGm7sL+eCZtvjQil7t505Gnr82oNdjOvv4L8WXQfwHYPuHbDLRHvgAAAABJRU5ErkJggg==",
              description: "ความปลอดภัยต่อผู้ขับขี่รถจักรยานยนต์ (MS)",
            },
          ];
          const ratingsContainer = document.getElementById("ratings-container");
 
          ratingData.forEach((data) => {
            const ratingBox = document.createElement("div");
            ratingBox.classList.add("rating-box");
 
            const canvas = document.createElement("canvas");
            canvas.width = 120;
            canvas.height = 120;
 
            const progressContent = document.createElement("div");
            progressContent.classList.add("progress-content");
 
            const icon = document.createElement("img");
            icon.classList.add("progress-icon");
            icon.src = data.icon;
            icon.alt = "Icon";
 
            const score = document.createElement("div");
            score.classList.add("score");
            score.innerText = data.value;
            score.style.color = data.color;
 
            const description = document.createElement("div");
            description.classList.add("description");
            description.innerText = data.description;
 
            progressContent.appendChild(icon);
            progressContent.appendChild(score);
            ratingBox.appendChild(canvas);
            ratingBox.appendChild(progressContent);
            ratingBox.appendChild(description);
            ratingsContainer.appendChild(ratingBox);
 
            animateCircularProgress(canvas, data.value, data.color);
          });
 
          function animateCircularProgress(canvas, value, color) {
            const ctx = canvas.getContext("2d");
            const startAngle = -0.5 * Math.PI;
            const maxAngle = (value / 50) * 2 * Math.PI + startAngle;
            let currentAngle = startAngle;
            const animationSpeed = 0.1; // Adjust for smoothness
 
            function lightenColor(hex, percent) {
              const num = parseInt(hex.slice(1), 16);
              const amt = Math.round(2.55 * percent);
              const r = (num >> 16) + amt;
              const g = ((num >> 8) & 0x00ff) + amt;
              const b = (num & 0x0000ff) + amt;
              return `rgb(${Math.min(r, 255)}, ${Math.min(g, 255)}, ${Math.min(b, 255)})`;
            }
 
            const lightColor = lightenColor(color, 60); // Light version of the progress color
            function drawFrame() {
              ctx.clearRect(0, 0, canvas.width, canvas.height);
              ctx.beginPath();
              ctx.arc(60, 60, 50, 0, 2 * Math.PI);
              ctx.lineWidth = 10;
              ctx.strokeStyle = lightColor;
              ctx.stroke();
 
              // Draw background circle
              ctx.beginPath();
              ctx.arc(60, 60, 50, 0, 2 * Math.PI);
              ctx.lineWidth = 10;
              ctx.strokeStyle = "#E0E0E0";
              ctx.stroke();
 
              // Draw animated progress circle
              ctx.beginPath();
              ctx.arc(60, 60, 50, startAngle, currentAngle);
              ctx.lineWidth = 10;
              ctx.strokeStyle = color;
              ctx.lineCap = "round";
              ctx.stroke();
 
              if (currentAngle < maxAngle) {
                currentAngle += animationSpeed;
                requestAnimationFrame(drawFrame);
              }
            }
            drawFrame();
          }
        });
      </script>
	<style>
		.container-ratting {
  display: flex;
  justify-content: center;
  align-items: center;

  background: #f9f9f9;
  padding: 30px;
  border-radius: 15px;
  position: relative;
}
.badge-section {
  width: 250px;
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
}
.badge {
  width: 190px;
  height: auto;
  position: absolute;
  top: -90px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 2;
}
.badge-text {
  margin-top: 120px;
}
.ratings-section {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-wrap: wrap;
  flex-grow: 1;
  padding: 10px;
  width: 100%;
  gap: 50px;
}
.rating-box {
  text-align: center;
  position: relative;
  width: 150px;
  margin: 10px;
}
canvas {
  width: 120px;
  height: 120px;
}
.progress-content {
  position: absolute;
  top: 35%;
  left: 50%;
  transform: translate(-50%, -50%);
  text-align: center;
}
.progress-icon {
  width: 42px;
  height: 42px;
}
.score {
  font-size: 20px;
  font-weight: 700;
  font-family: "roboto";
}
.description {
  margin-top: 12px;
  font-family: "Roboto";
  font-weight: 700;
  font-size: 14px;
  color: #262626;
  text-align: center;
  line-height: 20px;
}
.footer {
  font-size: 12px;
  color: #666;
  margin-top: 20px;
}
.badge-text a {
  box-sizing: border-box;
  line-height: 28px;
  font-size: 14px;
  font-family: Roboto;
  font-weight: 700;
  text-align: center;
  color: #576b95 !important;
  outline: none;
  border: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  text-decoration: none;
}
@media (max-width: 768px) {
  .container-ratting {
    display: flex;
    flex-direction: column;
    margin-left: 0px;
  }
  .badge {
    width: 136px;
    height: 136px;
  }
  .ratings-section {
    grid-template-columns: repeat(2, 1fr); /* 2 columns */
    gap: 15px;
    padding: 5px;
  }
  .rating-box {
    width: 120px;
  }
  .progress-content {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -50%);
    text-align: center;
  }
  .ratings-section {
    margin-left: 0px;
  }
  .description {
    font-size: 14px;
  }
  .badge-text {
    margin-top: 75px;
  }
}

</style>

    <?php
    return ob_get_clean(); // Return the buffered output
}
add_shortcode('crash_rating', 'crash_rating_shortcode');


