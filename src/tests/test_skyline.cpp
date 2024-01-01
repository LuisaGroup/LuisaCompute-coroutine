#include <luisa/core/clock.h>
#include <luisa/core/logging.h>
#include <luisa/runtime/context.h>
#include <luisa/runtime/device.h>
#include <luisa/runtime/stream.h>
#include <luisa/runtime/event.h>
#include <luisa/runtime/swapchain.h>
#include <luisa/dsl/syntax.h>
#include <luisa/gui/window.h>
#include <luisa/dsl/sugar.h>
#include <luisa/coro/coro_dispatcher.h>
using namespace luisa;
using namespace luisa::compute;
struct alignas(4) Skyline {
};
const bool SHOW = true;
LUISA_COROFRAME_STRUCT(Skyline){};
int main(int argc, char *argv[]) {

    Context context{argv[0]};

    if (argc <= 1) {
        LUISA_INFO("Usage: {} <backend>. <backend>: cuda, dx, cpu, metal", argv[0]);
        exit(1);
    }
    Device device = context.create_device(argv[1]);
    static constexpr uint width = 1024u;
    static constexpr uint height = 1024u;
    static constexpr uint __resx = 1024u;
    static constexpr uint __resy = 1024u;
    using vec3 = Float3;
    using vec2 = Float2;
    using vec4 = Float4;
    bool is_coroutine = false;
    auto RayTrace = [&](Float2 fragCoord, Float localTime) {
        Float seed = 1.0f;

        // Animation variables
        Float fade = 1.0f;
        vec3 sunDir;
        vec3 sunCol;
        Float exposure = 1.0f;
        vec3 skyCol, horizonCol;

        // other
        float marchCount = 0.0f;

        // ---- noise functions ----
        Callable v31 = [](vec3 a) {
            return a.x + a.y * 37.0f + a.z * 521.0f;
        };
        Callable v21 = [](vec2 a) {
            return a.x + a.y * 37.0f;
        };
        Callable Hash11 = [](Float a) {
            return fract(sin(a) * 10403.9f);
        };
        Callable Hash21 = [](vec2 uv) {
            auto f = uv.x + uv.y * 37.0f;
            return fract(sin(f) * 104003.9f);
        };
        Callable Hash22 = [](vec2 uv) {
            auto f = uv.x + uv.y * 37.0f;
            return fract(cos(f) * vec2(10003.579f, 37049.7f));
        };
        Callable Hash12 = [](Float f) {
            return fract(cos(f) * vec2(10003.579f, 37049.7f));
        };
        Callable Hash1d = [](Float u) {
            return fract(sin(u) * 143.9f);// scale this down to kill the jitters
        };
        Callable Hash2d = [](vec2 uv) {
            auto f = uv.x + uv.y * 37.0f;
            return fract(sin(f) * 104003.9f);
        };
        Callable Hash3d = [](vec3 uv) {
            auto f = uv.x + uv.y * 37.0f + uv.z * 521.0f;
            return fract(sin(f) * 110003.9f);
        };
        auto mix = [](auto x, auto y, Float a) {
            return x * (1.0f - a) + y * a;
        };
        Callable mixP = [&](Float f0, Float f1, Float a) {
            return mix(f0, f1, a * a * (3.0f - 2.0f * a));
        };
        const float2 zeroOne = float2(0.0f, 1.0f);
        Callable noise2d = [&](vec2 uv) {
            vec2 fr = fract(uv.xy());
            vec2 fl = floor(uv.xy());
            auto h00 = Hash2d(fl);
            auto h10 = Hash2d(fl + zeroOne.yx());
            auto h01 = Hash2d(fl + zeroOne);
            auto h11 = Hash2d(fl + zeroOne.yy());
            return mixP(mixP(h00, h10, fr.x), mixP(h01, h11, fr.x), fr.y);
        };
        Callable noise = [&](vec3 uv) {
            vec3 fr = fract(uv.xyz());
            vec3 fl = floor(uv.xyz());
            auto h000 = Hash3d(fl);
            auto h100 = Hash3d(fl + zeroOne.yxx());
            auto h010 = Hash3d(fl + zeroOne.xyx());
            auto h110 = Hash3d(fl + zeroOne.yyx());
            auto h001 = Hash3d(fl + zeroOne.xxy());
            auto h101 = Hash3d(fl + zeroOne.yxy());
            auto h011 = Hash3d(fl + zeroOne.xyy());
            auto h111 = Hash3d(fl + zeroOne.yyy());
            return mixP(
                mixP(mixP(h000, h100, fr.x),
                     mixP(h010, h110, fr.x), fr.y),
                mixP(mixP(h001, h101, fr.x),
                     mixP(h011, h111, fr.x), fr.y),
                fr.z);
        };

        const float PI = 3.14159265;
        auto saturate = [](auto a) {
            return max(0.0f, min(1.0f, a));
        };
        // This function basically is a procedural environment map that makes the sun
        Callable GetSunColorSmall = [&](vec3 rayDir, vec3 sunDir) {
            vec3 localRay = normalize(rayDir);
            auto dist = 1.0f - (dot(localRay, sunDir) * 0.5f + 0.5f);
            auto sunIntensity = 0.05f / dist;
            sunIntensity += exp(-dist * 150.0f) * 7000.0f;
            sunIntensity = min(sunIntensity, 40000.0f);
            return sunCol * sunIntensity * 0.025f;
        };

        Callable GetEnvMap = [&](vec3 rayDir, vec3 sunDir) {
            // fade the sky color, multiply sunset dimming
            vec3 finalColor = mix(horizonCol, skyCol, pow(saturate(rayDir.y), 0.47f)) * 0.95f;
            // make clouds - just a horizontal plane with noise
            auto n = noise2d(rayDir.xz() / rayDir.y * 1.0f);
            n += noise2d(rayDir.xz() / rayDir.y * 2.0f) * 0.5f;
            n += noise2d(rayDir.xz() / rayDir.y * 4.0f) * 0.25f;
            n += noise2d(rayDir.xz() / rayDir.y * 8.0f) * 0.125f;
            n = pow(abs(n), 3.0);
            n = mix(n * 0.2f, n, saturate(abs(rayDir.y * 8.f)));// fade clouds in distance
            finalColor = mix(finalColor, (make_float3(1.0) + sunCol * 10.0f) * 0.75f * saturate((rayDir.y + 0.2f) * 5.0f), saturate(n * 0.125f));

            // add the sun
            finalColor += GetSunColorSmall(rayDir, sunDir);
            return finalColor;
        };
        Callable GetEnvMapSkyline = [&](vec3 rayDir, vec3 sunDir, Float height) {
            vec3 finalColor = GetEnvMap(rayDir, sunDir);

            // Make a skyscraper skyline reflection.
            Float radial = atan2(rayDir.z, rayDir.x) * 4.0f;
            auto skyline = floor((sin(5.3456f * radial) + sin(1.234f * radial) + sin(2.177f * radial)) * 0.6f);
            radial *= 4.0f;
            skyline += floor((sin(5.0f * radial) + sin(1.234f * radial) + sin(2.177f * radial)) * 0.6f) * 0.1f;
            auto mask = saturate((rayDir.y * 8.0f - skyline - 2.5f + height) * 24.0f);
            auto vert = sign(sin(radial * 32.0f)) * 0.5f + 0.5f;
            auto hor = sign(sin(rayDir.y * 256.0f)) * 0.5f + 0.5f;
            mask = saturate(mask + (1.0f - hor * vert) * 0.05f);
            finalColor = mix(finalColor * make_float3(0.1f, 0.07f, 0.05f), finalColor, mask);

            return finalColor;
        };

        // min function that supports materials in the y component
        Callable matmin = [](vec2 a, vec2 b) {
            return ite(a.x < b.x, a, b);
        };

        // ---- shapes defined by distance fields ----
        // See this site for a reference to more distance functions...
        // https://iquilezles.org/articles/distfunctions

        // signed box distance field
        Callable sdBox = [&](vec3 p, vec3 radius) {
            vec3 dist = abs(p) - radius;
            return min(max(dist.x, max(dist.y, dist.z)), 0.0f) + length(max(dist, 0.0f));
        };

        // capped cylinder distance field
        Callable cylCap = [&](vec3 p, Float r, Float lenRad) {
            Float a = length(p.xy()) - r;
            a = max(a, abs(p.z) - lenRad);
            return a;
        };

        // k should be negative. -4.0 works nicely.
        // smooth blending function
        Callable smin = [&](Float a, Float b, Float k) {
            return log2(exp2(k * a) + exp2(k * b)) / k;
        };

        Callable Repeat = [&](Float a, Float len) {
            return mod(a, len) - 0.5f * len;
        };

        // Distance function that defines the car.
        // Basically it's 2 boxes smooth-blended together and a mirrored cylinder for the wheels.
        Callable Car = [&](vec3 baseCenter, Float unique) {
            // bottom box
            Float car = sdBox(baseCenter + make_float3(0.0f, -0.008f, 0.001f), make_float3(0.01f, 0.00225f, 0.0275f));
            // top box smooth blended
            car = smin(car, sdBox(baseCenter + make_float3(0.0f, -0.016f, 0.008f), make_float3(0.005f, 0.0005f, 0.01f)), -160.0f);
            // mirror the z axis to duplicate the cylinders for wheels
            vec3 wMirror = baseCenter + make_float3(0.0f, -0.005f, 0.0f);
            wMirror.z = abs(wMirror.z) - 0.02f;
            Float wheels = cylCap((wMirror).zyx(), 0.004f, 0.0135f);
            // Set materials
            vec2 distAndMat = make_float2(wheels, 3.0f);// car wheels
            // Car material is some big number that's unique to each car
            // so I can have each car be a different color
            distAndMat = matmin(distAndMat, vec2(car, 100000.0f + unique));// car
            return distAndMat;
        };

        // How much space between voxel borders and geometry for voxel ray march optimization
        float voxelPad = 0.2f;
        // p should be in [0..1] range on xz plane
        // pint is an integer pair saying which city block you are on
        Callable CityBlock = [&](vec3 p, vec2 pint) {
            // Get random numbers for this block by hashing the city block variable
            vec4 rand;
            rand = make_float4(Hash22(pint), Hash22(Hash22(pint)));
            vec2 rand2 = Hash22(rand.zw());

            // Radius of the building
            Float baseRad = 0.2f + (rand.x) * 0.1f;
            baseRad = floor(baseRad * 20.0f + 0.5f) / 20.0f;// try to snap this for window texture

            // make position relative to the middle of the block
            vec3 baseCenter = p - make_float3(0.5, 0.0, 0.5);
            Float height = rand.w * rand.z + 0.1f;// height of first building block
            // Make the city skyline higher in the middle of the city.
            Float downtown = saturate(4.0f / length(pint.xy()));
            height *= downtown;
            height *= 1.5f + (baseRad - 0.15f) * 20.0f;
            height += 0.1f;// minimum building height
            //height += sin(iTime + pint.x);	// animate the building heights if you're feeling silly
            height = floor(height * 20.0f) * 0.05f;                            // height is in floor units - each floor is 0.05 high.
            Float d = sdBox(baseCenter, make_float3(baseRad, height, baseRad));// large building piece

            // road
            d = min(d, p.y);

            //if (length(pint.xy) > 8.0) return vec2(d, mat);	// Hack to LOD in the distance

            // height of second building section
            auto height2 = max(0.0f, rand.y * 2.0f - 1.0f) * downtown;
            height2 = floor(height2 * 20.0f) * 0.05f;// floor units
            rand2 = floor(rand2 * 20.0f) * 0.05f;    // floor units
                                                     // size pieces of building
            d = min(d, sdBox(baseCenter - make_float3(0.0f, height, 0.0f), make_float3(baseRad, height2 - rand2.y, baseRad * 0.4f)));
            d = min(d, sdBox(baseCenter - make_float3(0.0f, height, 0.0f), make_float3(baseRad * 0.4f, height2 - rand2.x, baseRad)));
            // second building section
            $if (rand2.y > 0.25f) {
                d = min(d, sdBox(baseCenter - make_float3(0.0f, height, 0.0f), make_float3(baseRad * 0.8f, height2, baseRad * 0.8f)));
                // subtract off piece from top so it looks like there's a wall around the roof.
                Float topWidth = baseRad;
                $if (height2 > 0.0f) {
                    topWidth = baseRad * 0.8f;
                };
                d = max(d, -sdBox(baseCenter - make_float3(0.0f, height + height2, 0.0f), make_float3(topWidth - 0.0125f, 0.015f, topWidth - 0.0125f)));
            }
            $else {
                // Cylinder top section of building
                $if (height2 > 0.0f) {
                    d = min(d, cylCap((baseCenter - make_float3(0.0f, height, 0.0f)).xzy(), baseRad * 0.8f, height2));
                };
            };
            // mini elevator shaft boxes on top of building
            d = min(d, sdBox(baseCenter - make_float3((rand.x - 0.5f) * baseRad, height + height2, (rand.y - 0.5f) * baseRad),
                             make_float3(baseRad * 0.3f * rand.z, 0.1f * rand2.y, baseRad * 0.3f * rand2.x + 0.025f)));
            // mirror another box (and scale it) so we get 2 boxes for the price of 1.
            vec3 boxPos = baseCenter - make_float3((rand2.x - 0.5f) * baseRad, height + height2, (rand2.y - 0.5f) * baseRad);
            Float big = sign(boxPos.x);
            boxPos.x = abs(boxPos.x) - 0.02f - baseRad * 0.3f * rand.w;
            d = min(d, sdBox(boxPos,
                             make_float3(baseRad * 0.3f * rand.w, 0.07f * rand.y, baseRad * 0.2f * rand.x + big * 0.025f)));

            // Put domes on some building tops for variety
            $if (rand.y < 0.04f) {
                d = min(d, length(baseCenter - make_float3(0.0f, height, 0.0f)) - baseRad * 0.8f);
            };

            //d = max(d, p.y);  // flatten the city for debugging cars

            // Need to make a material variable.
            vec2 distAndMat = make_float2(d, 0.0f);
            // sidewalk box with material
            distAndMat = matmin(distAndMat, make_float2(sdBox(baseCenter, make_float3(0.35, 0.005, 0.35)), 1.0f));

            return distAndMat;
        };

        // This is the distance function that defines all the scene's geometry.
        // The input is a position in space.
        // The output is the distance to the nearest surface and a material index.
        Callable DistanceToObject = [&](vec3 p) {
            //p.y += noise2d((p.xz)*0.0625)*8.0; // Hills
            vec3 rep = p;
            rep.x = fract(p.x);// [0..1] for representing the position in the city block
            rep.z = fract(p.z);// [0..1] for representing the position in the city block
            vec2 distAndMat = CityBlock(rep, floor(p.xz()));

            // Set up the cars. This is doing a lot of mirroring and repeating because I
            // only want to do a single call to the car distance function for all the
            // cars in the scene. And there's a lot of traffic!
            vec3 p2 = p;
            rep = p2;
            Float carTime = localTime * 0.2f;// Speed of car driving
            Float crossStreet = 1.0f;        // whether we are north/south or east/west
            Float repeatDist = 0.25;         // Car density bumper to bumper
            // If we are going north/south instead of east/west (?) make cars that are
            // stopped in the street so we don't have collisions.
            $if (abs(fract(rep.x) - 0.5f) < 0.35f) {
                p2.x += 0.05f;
                auto zz = p2.x * 1.0f;// Rotate 90 degrees
                auto xx = p2.z * -1.0f;
                p2.z = zz;
                p2.x = xx;// Rotate 90 degrees
                rep.x = p2.x;
                rep.z = p2.z;
                crossStreet = 0.0f;
                repeatDist = 0.1f;// Denser traffic on cross streets
            };

            rep.z += floor(p2.x);             // shift so less repitition between parallel blocks
            rep.x = Repeat(p2.x - 0.5f, 1.0f);// repeat every block
            rep.z = rep.z * sign(rep.x);      // mirror but keep cars facing the right way
            rep.x = (rep.x * sign(rep.x)) - 0.09f;
            rep.z -= carTime * crossStreet;              // make cars move
            Float uniqueID = floor(rep.z / repeatDist);  // each car gets a unique ID that we can use for colors
            rep.z = Repeat(rep.z, repeatDist);           // repeat the line of cars every quarter block
            rep.x += (Hash11(uniqueID) * 0.075f - 0.01f);// nudge cars left and right to take both lanes
            Float frontBack = Hash11(uniqueID * 0.987f) * 0.18f - 0.09f;
            frontBack *= sin(localTime * 2.0f + uniqueID);
            rep.z += frontBack * crossStreet; // nudge cars forward back for variation
            vec2 carDist = Car(rep, uniqueID);// car distance function

            // Drop the cars in the scene with materials
            distAndMat = matmin(distAndMat, carDist);

            return distAndMat;
        };

        // This basically makes a procedural texture map for the sides of the buildings.
        // It makes a texture, a normal for normal mapping, and a mask for window reflection.
        Callable CalcWindows = [&](vec2 block, vec3 pos, vec3 &texColor, Float &windowRef, vec3 &normal) {
            vec3 hue = make_float3(Hash21(block) * 0.8f, Hash21(block * 7.89f) * 0.4f, Hash21(block * 37.89f) * 0.5f);
            texColor += hue * 0.4f;
            texColor *= 0.75f;
            Float window = 0.0f;
            window = max(window, mix(0.2f, 1.0f, floor(fract(pos.y * 20.0f - 0.35f) * 2.0f + 0.1f)));
            $if (pos.y < 0.05f) {
                window = 1.0f;
            };
            Float winWidth = Hash21(block * 4.321f) * 2.0f;
            $if ((winWidth < 1.3f) & (winWidth >= 1.0f)) {
                winWidth = 1.3f;
            };
            window = max(window, mix(0.2f, 1.0f, floor(fract(pos.x * 40.0f + 0.05f) * winWidth)));
            window = max(window, mix(0.2f, 1.0f, floor(fract(pos.z * 40.0f + 0.05f) * winWidth)));
            $if (window < 0.5f) {
                windowRef += 1.0f;
            };
            window *= Hash21(block * 1.123f);
            texColor *= window;

            Float wave = floor(sin((pos.y * 40.0f - 0.1f) * PI) * 0.505f - 0.5f) + 1.0f;
            normal.y -= max(-1.0f, min(1.0f, -wave * 0.5f));
            Float pits = min(1.0f, abs(sin((pos.z * 80.0f) * PI)) * 4.f) - 1.0f;
            normal.z += pits * 0.25f;
            pits = min(1.0f, abs(sin((pos.x * 80.0f) * PI)) * 4.0f) - 1.0f;
            normal.x += pits * 0.25f;
        };

        // Input is UV coordinate of pixel to render.
        // Output is RGB color.
        marchCount = 0.0f;
        // -------------------------------- animate ---------------------------------------
        sunCol = make_float3(258.0f, 248.0f, 200.0f) / 3555.0f;
        sunDir = normalize(make_float3(0.93f, 1.0f, 1.0f));
        horizonCol = make_float3(1.0f, 0.95f, 0.85f) * 0.9f;
        skyCol = make_float3(0.3f, 0.5f, 0.95f);
        exposure = def<float>(1.0f);

        vec3 camPos, camUp, camLookat;
        // ------------------- Set up the camera rays for ray marching --------------------
        // Map uv to [-1.0..1.0]
        vec2 uv = fragCoord.xy() / make_float2(__resx, __resy) * 2.0f - 1.0f;
        uv.y = -uv.y;
        uv /= 2.0f;// zoom in
        // Do the camera fly-by animation and different scenes.
        // Time variables for start and end of each scene
        const float t0 = 0.0;
        const float t1 = 8.0;
        const float t2 = 14.0;
        const float t3 = 24.0;
        const float t4 = 38.0;
        const float t5 = 56.0;
        const float t6 = 58.0;
        /*const float t0 = 0.0;
    const float t1 = 0.0;
    const float t2 = 0.0;
    const float t3 = 0.0;
    const float t4 = 0.0;
    const float t5 = 16.0;
    const float t6 = 18.0;*/
        // Repeat the animation after time t6
        localTime = fract(localTime / t6) * t6;
        $if (localTime < t1) {
            auto time = localTime - t0;
            auto alpha = time / (t1 - t0);
            fade = saturate(time);
            fade *= saturate(t1 - localTime);
            camPos = make_float3(13.0f, 3.3f, -3.5f);
            camPos.x -= smoothstep(0.0f, 1.0f, alpha) * 4.8f;
            camUp = make_float3(0, 1, 0);
            camLookat = make_float3(0, 1.5, 1.5);
        }
        $else {
            $if (localTime < t2) {
                auto time = localTime - t1;
                auto alpha = time / (t2 - t1);
                fade = saturate(time);
                fade *= saturate(t2 - localTime);
                camPos = make_float3(26.0f, 0.05f + smoothstep(0.0f, 1.0f, alpha) * 0.4f, 2.0f);
                camPos.z -= alpha * 2.8f;
                camUp = make_float3(0, 1, 0);
                camLookat = make_float3(camPos.x - 0.3f, -8.15f, -40.0f);

                sunDir = normalize(make_float3(0.95, 0.6, 1.0));
                sunCol = make_float3(258.0, 248.0, 160.0) / 3555.0f;
                exposure *= 0.7f;
                skyCol *= 1.5f;
            }
            $else {
                $if (localTime < t3) {
                    auto time = localTime - t2;
                    auto alpha = time / (t3 - t2);
                    fade = saturate(time);
                    fade *= saturate(t3 - localTime);
                    camPos = make_float3(12.0, 6.3, -0.5);
                    camPos.y -= alpha * 5.5f;
                    camPos.x = cos(alpha * 1.0f) * 5.2f;
                    camPos.z = sin(alpha * 1.0f) * 5.2f;
                    camUp = normalize(make_float3(0.f, 1.f, -0.5f + alpha * 0.5f));
                    camLookat = make_float3(0, 1.0, -0.5);
                }
                $else {
                    $if (localTime < t4) {
                        auto time = localTime - t3;
                        auto alpha = time / (t4 - t3);
                        fade = saturate(time);
                        fade *= saturate(t4 - localTime);
                        camPos = make_float3(2.15f - alpha * 0.5f, 0.02f, -1.0f - alpha * 0.2f);
                        camPos.y += smoothstep(0.0f, 1.0f, alpha * alpha) * 3.4f;
                        camUp = normalize(make_float3(0, 1, 0.0));
                        camLookat = make_float3(0.f, 0.5f + alpha, alpha * 5.0f);
                    }
                    $else {
                        $if (localTime < t5) {
                            auto time = localTime - t4;
                            auto alpha = time / (t5 - t4);
                            fade = saturate(time);
                            fade *= saturate(t5 - localTime);
                            camPos = make_float3(-2.0f, 1.3f - alpha * 1.2f, -10.5f - alpha * 0.5f);
                            camUp = normalize(make_float3(0, 1, 0.0));
                            camLookat = make_float3(-2.0f, 0.3f + alpha, -0.0f);
                            sunDir = normalize(make_float3(0.5f - alpha * 0.6f, 0.3f - alpha * 0.3f, 1.0f));
                            sunCol = make_float3(258.0, 148.0, 60.0) / 3555.0f;
                            localTime *= 16.0f;
                            exposure *= 0.4f;
                            horizonCol = make_float3(1.0f, 0.5f, 0.35f) * 2.0f;
                            skyCol = make_float3(0.75f, 0.5f, 0.95f);
                        }
                        $else {
                            $if (localTime < t6) {
                                fade = 0.0f;
                                camPos = make_float3(26.0f, 100.0f, 2.0f);
                                camUp = make_float3(0, 1, 0);
                                camLookat = make_float3(0.3f, 0.15f, 0.0f);
                            };
                        };
                    };
                };
            };
        };

        // Camera setup for ray tracing / marching
        vec3 camVec = normalize(camLookat - camPos);
        vec3 sideNorm = normalize(cross(camUp, camVec));
        vec3 upNorm = cross(camVec, sideNorm);
        vec3 worldFacing = (camPos + camVec);
        vec3 worldPix = worldFacing + uv.x * sideNorm * def<float>(__resx / __resy) + uv.y * upNorm;
        vec3 rayVec = normalize(worldPix - camPos);

        // ----------------------------- Ray march the scene ------------------------------
        vec2 distAndMat;             // Distance and material
        Float t = 0.05f;             // + Hash2d(uv)*0.1;	// random dither-fade things close to the camera
        const float maxDepth = 45.0f;// farthest distance rays will travel
        vec3 pos = make_float3(0.0f);
        const float smallVal = 0.00000625;
        // ray marching time
        $for (i, 0, 500)// This is the count of the max times the ray actually marches.
        {
            //            if (is_coroutine) $suspend("ray_march");
            marchCount += 1.0f;
            // Step along the ray.
            pos = (camPos + rayVec * t);
            // This is _the_ function that defines the "distance field".
            // It's really what makes the scene geometry. The idea is that the
            // distance field returns the distance to the closest object, and then
            // we know we are safe to "march" along the ray by that much distance
            // without hitting anything. We repeat this until we get really close
            // and then break because we have effectively hit the object.
            distAndMat = DistanceToObject(pos);

            // 2d voxel walk through the city blocks.
            // The distance function is not continuous at city block boundaries,
            // so we have to pause our ray march at each voxel boundary.
            auto walk = distAndMat.x;
            auto dx = -fract(pos.x);
            $if (rayVec.x > 0.0f) {

                dx = fract(-pos.x);
            };
            auto dz = -fract(pos.z);
            $if (rayVec.z > 0.0f) {
                dz = fract(-pos.z);
            };
            auto nearestVoxel = min(fract(dx / rayVec.x), fract(dz / rayVec.z)) + voxelPad;
            nearestVoxel = max(voxelPad, nearestVoxel);// hack that assumes streets and sidewalks are this wide.
            //nearestVoxel = max(nearestVoxel, t * 0.02); // hack to stop voxel walking in the distance.
            walk = min(walk, nearestVoxel);

            // move down the ray a safe amount
            t += walk;
            // If we are very close to the object, let's call it a hit and exit this loop.
            $if ((t > maxDepth) | (abs(distAndMat.x) < smallVal)) {

                $break;
            };
        };
        // Ray trace a ground plane to infinity
        auto alpha = -camPos.y / rayVec.y;
        $if ((t > maxDepth) & (rayVec.y < -0.0f)) {
            pos.x = camPos.x + rayVec.x * alpha;
            pos.z = camPos.z + rayVec.z * alpha;
            pos.y = -0.0f;
            t = alpha;
            distAndMat.y = 0.0f;
            distAndMat.x = 0.0f;
        };
        // --------------------------------------------------------------------------------
        // Now that we have done our ray marching, let's put some color on this geometry.
        vec3 finalColor = make_float3(0.0);

        // If a ray actually hit the object, let's light it.
        $if ((t <= maxDepth) | (t == alpha)) {
            //if (is_coroutine) $suspend("hit_object");
            auto dist = distAndMat.x;
            // calculate the normal from the distance field. The distance field is a volume, so if you
            // sample the current point and neighboring points, you can use the difference to get
            // the normal.
            vec3 smallVec = make_float3(smallVal, 0, 0);
            vec3 normalU = make_float3(dist - DistanceToObject(pos - smallVec.xyy()).x,
                                       dist - DistanceToObject(pos - smallVec.yxy()).x,
                                       dist - DistanceToObject(pos - smallVec.yyx()).x);
            vec3 normal = normalize(normalU);

            // calculate 2 ambient occlusion values. One for global stuff and one
            // for local stuff
            auto ambientS = def<float>(1.0f);
            ambientS *= saturate(DistanceToObject(pos + normal * 0.0125f).x * 80.0f);
            ambientS *= saturate(DistanceToObject(pos + normal * 0.025f).x * 40.0f);
            ambientS *= saturate(DistanceToObject(pos + normal * 0.05f).x * 20.0f);
            ambientS *= saturate(DistanceToObject(pos + normal * 0.1f).x * 10.0f);
            ambientS *= saturate(DistanceToObject(pos + normal * 0.2f).x * 5.0f);
            ambientS *= saturate(DistanceToObject(pos + normal * 0.4f).x * 2.5f);
            //ambientS *= saturate(DistanceToObject(pos + normal * 0.8).x*1.25);
            auto ambient = ambientS;// * saturate(DistanceToObject(pos + normal * 1.6).x*1.25*0.5);
            //ambient *= saturate(DistanceToObject(pos + normal * 3.2)*1.25*0.25);
            //ambient *= saturate(DistanceToObject(pos + normal * 6.4)*1.25*0.125);
            ambient = max(0.025f, pow(ambient, 0.5f));// tone down ambient with a pow and min clamp it.
            ambient = saturate(ambient);

            // calculate the reflection vector for highlights
            vec3 ref = reflect(rayVec, normal);

            // Trace a ray toward the sun for sun shadows
            auto sunShadow = def<float>(1.0f);
            auto iter = def<float>(0.01f);
            vec3 nudgePos = pos + normal * 0.002f;// don't start tracing too close or inside the object
            $for (i, 0, 40) {
                //if (is_coroutine) $suspend("sun");
                vec3 shadowPos = nudgePos + sunDir * iter;
                auto tempDist = DistanceToObject(shadowPos).x;
                sunShadow *= saturate(tempDist * 150.0f);// Shadow hardness
                $if (tempDist <= 0.0f) {

                    $break;
                };

                auto walk = tempDist;
                auto dx = -fract(shadowPos.x);
                $if (sunDir.x > 0.0f) {
                    dx = fract(-shadowPos.x);
                };
                auto dz = -fract(shadowPos.z);
                $if (sunDir.z > 0.0f) {
                    dz = fract(-shadowPos.z);
                };
                auto nearestVoxel = min(fract(dx / sunDir.x), fract(dz / sunDir.z)) + smallVal;
                nearestVoxel = max(0.2f, nearestVoxel);// hack that assumes streets and sidewalks are this wide.
                walk = min(walk, nearestVoxel);

                iter += max(0.01f, walk);
                $if (iter > 4.5f) {
                    $break;
                };
            };
            sunShadow = saturate(sunShadow);

            // make a few frequencies of noise to give it some texture
            auto n = def<float>(0.0);
            n += noise(pos * 32.0f);
            n += noise(pos * 64.0f);
            n += noise(pos * 128.0f);
            n += noise(pos * 256.0f);
            n += noise(pos * 512.0f);
            n = mix(0.7f, 0.95f, n);

            // ------ Calculate texture color  ------
            vec2 block = floor(pos.xz());
            vec3 texColor = make_float3(0.95f, 1.0f, 1.0f);
            texColor *= 0.8f;
            auto windowRef = def<float>(0.0f);
            // texture map the sides of buildings
            $if ((normal.y < 0.1f) & (distAndMat.y == 0.0f)) {
                //                if (is_coroutine) $suspend("building");
                vec3 posdx = make_float3(0.0f);
                vec3 posdy = make_float3(0.0f);
                vec3 posGrad = posdx * Hash21(uv) + posdy * Hash21(uv * 7.6543f);

                // Quincunx antialias the building texture and normal map.
                // I guess procedural textures are hard to mipmap.
                vec3 colTotal = make_float3(0.0);
                vec3 colTemp = texColor;
                vec3 nTemp = make_float3(0.0);
                CalcWindows(block, pos, colTemp, windowRef, nTemp);
                colTotal = colTemp;

                colTemp = texColor;
                CalcWindows(block, pos + posdx * 0.666f, colTemp, windowRef, nTemp);
                colTotal += colTemp;

                colTemp = texColor;
                CalcWindows(block, pos + posdx * 0.666f + posdy * 0.666f, colTemp, windowRef, nTemp);
                colTotal += colTemp;

                colTemp = texColor;
                CalcWindows(block, pos + posdy * 0.666f, colTemp, windowRef, nTemp);
                colTotal += colTemp;

                colTemp = texColor;
                CalcWindows(block, pos + posdx * 0.333f + posdy * 0.333f, colTemp, windowRef, nTemp);
                colTotal += colTemp;

                texColor = colTotal * 0.2f;
                windowRef *= 0.2f;

                normal = normalize(normal + nTemp * 0.2f);
            }
            $else {
                //                if (is_coroutine) $suspend("road");
                // Draw the road
                Float xroad = abs(fract(pos.x + 0.5f) - 0.5f);
                Float zroad = abs(fract(pos.z + 0.5f) - 0.5f);
                Float road = saturate((min(xroad, zroad) - 0.143f) * 480.0f);
                texColor *= 1.0f - normal.y * 0.95f * Hash21(block * 9.87f) * road;// change rooftop color
                texColor *= mix(0.1f, 1.0f, road);

                // double yellow line in middle of road
                Float yellowLine = saturate(1.0f - (min(xroad, zroad) - 0.002f) * 480.0f);
                yellowLine *= saturate((min(xroad, zroad) - 0.0005f) * 480.0f);
                yellowLine *= saturate((xroad * xroad + zroad * zroad - 0.05f) * 880.0f);
                texColor = mix(texColor, make_float3(1.0, 0.8, 0.3), yellowLine);

                // white dashed lines on road
                Float whiteLine = saturate(1.f - (min(xroad, zroad) - 0.06f) * 480.0f);
                whiteLine *= saturate((min(xroad, zroad) - 0.056f) * 480.0f);
                whiteLine *= saturate((xroad * xroad + zroad * zroad - 0.05f) * 880.0f);
                whiteLine *= saturate(1.0f - (fract(zroad * 8.0f) - 0.5f) * 280.0f);// dotted line
                whiteLine *= saturate(1.0f - (fract(xroad * 8.0f) - 0.5f) * 280.0f);
                texColor = mix(texColor, make_float3(0.5), whiteLine);

                whiteLine = saturate(1.0f - (min(xroad, zroad) - 0.11f) * 480.0f);
                whiteLine *= saturate((min(xroad, zroad) - 0.106f) * 480.0f);
                whiteLine *= saturate((xroad * xroad + zroad * zroad - 0.06f) * 880.0f);
                texColor = mix(texColor, make_float3(0.5f), whiteLine);

                // crosswalk
                Float crossWalk = saturate(1.0f - (fract(xroad * 40.0f) - 0.5f) * 280.0f);
                crossWalk *= saturate((zroad - 0.15f) * 880.0f);
                crossWalk *= saturate((-zroad + 0.21f) * 880.0f) * (1.0f - road);
                crossWalk *= n * n;
                texColor = mix(texColor, make_float3(0.25), crossWalk);
                crossWalk = saturate(1.0f - (fract(zroad * 40.0f) - 0.5f) * 280.0f);
                crossWalk *= saturate((xroad - 0.15f) * 880.0f);
                crossWalk *= saturate((-xroad + 0.21f) * 880.0f) * (1.0f - road);
                crossWalk *= n * n;
                texColor = mix(texColor, make_float3(0.25), crossWalk);

                {
                    // sidewalk cracks
                    Float sidewalk = 1.0f;
                    vec2 blockSize = make_float2(100.0f);
                    $if (pos.y > 0.1f) {
                        blockSize = make_float2(10.0, 50);
                    };
                    //sidewalk *= pow(abs(sin(pos.x*blockSize)), 0.025);
                    //sidewalk *= pow(abs(sin(pos.z*blockSize)), 0.025);
                    sidewalk *= saturate(abs(sin(pos.z * blockSize.x) * 800.0f / blockSize.x));
                    sidewalk *= saturate(abs(sin(pos.x * blockSize.y) * 800.0f / blockSize.y));
                    sidewalk = saturate(mix(0.7f, 1.0f, sidewalk));
                    sidewalk = saturate((1.0f - road) + sidewalk);
                    texColor *= sidewalk;
                }
            };
            //if (is_coroutine) $suspend("after");
            // Car tires are almost black to not call attention to their ugly.
            $if (distAndMat.y == 3.0f) {
                texColor = make_float3(0.05);
            };

            // apply noise
            texColor *= make_float3(1.0f) * n * 0.05f;
            texColor *= 0.7f;
            texColor = saturate(texColor);

            Float windowMask = 0.0f;
            $if (distAndMat.y >= 100.0f) {
                // car texture and windows
                texColor = make_float3(Hash11(distAndMat.y) * 1.0f, Hash11(distAndMat.y * 8.765f), Hash11(distAndMat.y * 17.731f)) * 0.1f;
                texColor = pow(abs(texColor), make_float3(0.2f));// bias toward white
                texColor = max(make_float3(0.25f), texColor);    // not too saturated color.
                texColor.z = min(texColor.y, texColor.z);        // no purple cars. just not realistic. :)
                texColor *= Hash11(distAndMat.y * 0.789f) * 0.15f;
                windowMask = saturate(max(0.0f, abs(pos.y - 0.0175f) * 3800.0f) - 10.0f);
                vec2 dirNorm = abs(normalize(normal.xz()));
                Float pillars = saturate(1.0f - max(dirNorm.x, dirNorm.y));
                pillars = pow(max(0.0f, pillars - 0.15f), 0.125f);
                windowMask = max(windowMask, pillars);
                texColor *= windowMask;
            };

            // ------ Calculate lighting color ------
            // Start with sun color, standard lighting equation, and shadow
            vec3 lightColor = make_float3(100.0) * sunCol * saturate(dot(sunDir, normal)) * sunShadow;
            // weighted average the near ambient occlusion with the far for just the right look
            Float ambientAvg = (ambient * 3.0f + ambientS) * 0.25f;
            // Add sky color with ambient acclusion
            lightColor += (skyCol * saturate(normal.y * 0.5f + 0.5f)) * pow(ambientAvg, 0.35f) * 2.5f;
            lightColor *= 4.0f;

            // finally, apply the light to the texture.
            finalColor = texColor * lightColor;
            // Reflections for cars
            $if (distAndMat.y >= 100.0f) {
                Float yfade = max(0.01f, min(1.0f, ref.y * 100.0f));
                // low-res way of making lines at the edges of car windows. Not sure I like it.
                //yfade *= (saturate(1.0f - abs(dFdx(windowMask) * dFdy(windowMask)) * 250.995f));
                finalColor += GetEnvMapSkyline(ref, sunDir, pos.y - 1.5f) * 0.3f * yfade * max(0.4f, sunShadow);
                //finalColor += saturate(texture(iChannel0, ref).xyz - 0.35f) * 0.15f * max(0.2f, sunShadow);
            };
            // reflections for building windows
            $if (windowRef != 0.0f) {
                finalColor *= mix(1.0f, 0.6f, windowRef);
                auto yfade = max(0.01f, min(1.0f, ref.y * 100.0f));
                finalColor += GetEnvMapSkyline(ref, sunDir, pos.y - 0.5f) * 0.6f * yfade * max(0.6f, sunShadow) * windowRef;//*(windowMask*0.5+0.5);
                //finalColor += saturate(texture(iChannel0, ref).xyz - 0.35f) * 0.15f * max(0.25f, sunShadow) * windowRef;
            };
            finalColor *= 0.9f;
            // fog that fades to reddish plus the sun color so that fog is brightest towards sun
            vec3 rv2 = rayVec;
            rv2.y *= saturate(sign(rv2.y));
            vec3 fogColor = GetEnvMap(rv2, sunDir);
            fogColor = min(make_float3(9.0f), fogColor);
            finalColor = mix(fogColor, finalColor, exp(-t * 0.02f));

            // visualize length of gradient of distance field to check distance field correctness
            //finalColor = make_float3(0.5) * (length(normalU) / smallVec.x);
            //finalColor = make_float3(marchCount)/255.0;
        }
        $else {
            // Our ray trace hit nothing, so draw sky.
            finalColor = GetEnvMap(rayVec, sunDir);
        };

        // vignette?
        finalColor *= make_float3(1.0) * saturate(1.0f - length(uv / 2.5f));
        finalColor *= 1.3f * exposure;

        // output the final color without gamma correction - will do gamma later.
        return make_float3(saturate(finalColor) * saturate(fade + 0.2f));
    };
    is_coroutine = false;
    Stream stream = device.create_stream(StreamTag::GRAPHICS);
    Window window{"Display", make_uint2(width, height)};
    Swapchain swap_chain{device.create_swapchain(
        window.native_handle(),
        stream,
        window.size(),
        false, true, 2)};
    Image<float> device_image = device.create_image<float>(swap_chain.backend_storage(), width, height);
    Kernel2D main_kernel = [&](ImageFloat image, Float time) noexcept {
        set_block_size(128, 1, 1);
        auto xy = dispatch_id().xy();
        image.write(xy, make_float4(sqrt(RayTrace(make_float2(xy), time)), 1.0f));
    };
    is_coroutine = true;
    Coroutine coro = [&](Var<Skyline> &frame, Float time) noexcept {
        auto xy = make_uint2(coro_id().x % width, coro_id().x / width);
        device_image->write(xy, make_float4(sqrt(RayTrace(make_float2(xy), time)), 1.0f));
    };
    Kernel2D clear_kernel = [](ImageVar<float> image) noexcept {
        Var coord = dispatch_id().xy();
        Var rg = make_float2(coord) / make_float2(dispatch_size().xy());
        image.write(coord, make_float4(make_float2(0.3f, 0.4f), 0.5f, 1.0f));
    };
    Kernel2D<Image<float>, float> k = main_kernel;
    main_kernel = k;
    k = main_kernel;

    Shader2D<Image<float>> clear = device.compile(clear_kernel);
    Shader2D<Image<float>, float> shader = device.compile(k);

    stream << clear(device_image).dispatch(width, height);

    Clock clock;
    coro::SimpleCoroDispatcher Wdispatcher{&coro, device, width * height};
    //coro::PersistentCoroDispatcher Wdispatcher{&coro, device, stream, 256 * 256 * 2u, 96u, 2u, false};
    /*while (!window.should_close()) {
        window.poll_events();
        float time = static_cast<float>(clock.toc() * 1e-3);
        //stream << shader(device_image, time).dispatch(width, height)
        Wdispatcher(device_image, time, width * height);
        stream
            << Wdispatcher.await_all()
            << swap_chain.present(device_image);
    }*/
    float time = 0;
    int count = 0;
    float length = 6;
    clock.tic();
    while (time < length) {
        time = static_cast<float>(clock.toc() * 1e-3);
        count += 1;
        stream << shader(device_image, time).dispatch(width, height);
        if (SHOW) {
            stream << swap_chain.present(device_image);
        }
    }
    time = 0;
    stream << synchronize();
    LUISA_INFO("FPS: {}.", count / (clock.toc() * 1e-3));
    count = 0;
    clock.tic();
    while (time < length) {
        time = static_cast<float>(clock.toc() * 1e-3);
        count += 1;
        Wdispatcher(time, width * height);
        stream << Wdispatcher.await_all();
        if (SHOW) {
            stream << swap_chain.present(device_image);
        }
    }
    stream << synchronize();
    LUISA_INFO("FPS: {}.", count / (clock.toc() * 1e-3));

    stream << synchronize();
}
